// Package auth decides which tenant a request speaks for.
//
// Two mechanisms, because the gateway sits in front of two kinds of caller:
// an API key for machine clients it issued credentials to, and an HS256 JWT
// for callers whose identity is minted by something upstream. Both end at the
// same place -- a tenant id -- and the limiter never learns which was used.
//
// Deliberately not here: issuing tokens, refresh flows, OIDC discovery, or
// anything else that would turn a rate limiter into an identity provider. The
// gateway verifies an assertion somebody else made.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ErrNoCredentials means the request carried nothing to authenticate with.
var ErrNoCredentials = errors.New("auth: no credentials")

// ErrBadCredentials means it carried something, and that something is wrong.
// Kept distinct from ErrNoCredentials so a 401 can say which.
var ErrBadCredentials = errors.New("auth: invalid credentials")

// KeyResolver turns a presented API key into a tenant id.
type KeyResolver interface {
	TenantForKey(ctx context.Context, presented string) (string, error)
}

// Authenticator resolves the tenant a request speaks for.
type Authenticator struct {
	keys      KeyResolver
	jwtSecret []byte
	issuer    string
	audience  string
	// tenantClaim names the JWT claim carrying the tenant id.
	tenantClaim string
}

// Option configures an Authenticator.
type Option func(*Authenticator)

// WithAPIKeys enables API key authentication against a resolver.
func WithAPIKeys(r KeyResolver) Option { return func(a *Authenticator) { a.keys = r } }

// WithJWT enables HS256 bearer tokens signed with secret.
func WithJWT(secret []byte, issuer, audience, tenantClaim string) Option {
	return func(a *Authenticator) {
		a.jwtSecret = secret
		a.issuer = issuer
		a.audience = audience
		if tenantClaim != "" {
			a.tenantClaim = tenantClaim
		}
	}
}

// New builds an Authenticator. With no options it authenticates nothing and
// says so on every request, which is the correct behaviour for a gateway
// configured without credentials rather than a silent pass-through.
func New(opts ...Option) *Authenticator {
	a := &Authenticator{tenantClaim: "tid"}
	for _, o := range opts {
		o(a)
	}
	return a
}

// Enabled reports whether any mechanism is configured.
func (a *Authenticator) Enabled() bool { return a.keys != nil || len(a.jwtSecret) > 0 }

// Tenant authenticates a request and returns the tenant it speaks for.
//
// Order matters only in that an explicit API key header is checked before a
// bearer token; a request carrying both is using the key.
func (a *Authenticator) Tenant(ctx context.Context, r *http.Request) (string, error) {
	if a.keys != nil {
		if presented := apiKeyFrom(r); presented != "" {
			tenant, err := a.keys.TenantForKey(ctx, presented)
			if err != nil {
				return "", fmt.Errorf("%w: api key", ErrBadCredentials)
			}
			return tenant, nil
		}
	}
	if len(a.jwtSecret) > 0 {
		if token := bearerFrom(r); token != "" {
			return a.tenantFromJWT(token)
		}
	}
	return "", ErrNoCredentials
}

// apiKeyFrom reads the key from either header form.
func apiKeyFrom(r *http.Request) string {
	if k := r.Header.Get("X-API-Key"); k != "" {
		return k
	}
	const prefix = "ApiKey "
	if v := r.Header.Get("Authorization"); strings.HasPrefix(v, prefix) {
		return strings.TrimSpace(v[len(prefix):])
	}
	return ""
}

func bearerFrom(r *http.Request) string {
	const prefix = "Bearer "
	v := r.Header.Get("Authorization")
	if len(v) > len(prefix) && strings.EqualFold(v[:len(prefix)], prefix) {
		return strings.TrimSpace(v[len(prefix):])
	}
	return ""
}

func (a *Authenticator) tenantFromJWT(raw string) (string, error) {
	opts := []jwt.ParserOption{
		// Pin the algorithm. Accepting whatever the token's header asks for is
		// how "alg: none" and RS256-verified-as-HMAC forgeries work.
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithExpirationRequired(),
	}
	if a.issuer != "" {
		opts = append(opts, jwt.WithIssuer(a.issuer))
	}
	if a.audience != "" {
		opts = append(opts, jwt.WithAudience(a.audience))
	}

	token, err := jwt.Parse(raw, func(*jwt.Token) (any, error) { return a.jwtSecret, nil }, opts...)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrBadCredentials, err)
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return "", fmt.Errorf("%w: unexpected claims", ErrBadCredentials)
	}
	tenant, _ := claims[a.tenantClaim].(string)
	if tenant == "" {
		return "", fmt.Errorf("%w: token carries no %q claim", ErrBadCredentials, a.tenantClaim)
	}
	return tenant, nil
}

// GeneratedKey is a freshly minted API key. The secret is returned once and
// never stored: what the store holds is the hash.
type GeneratedKey struct {
	Secret string
	Hash   []byte
	Prefix string
}

// KeyPrefixLen is how much of a key is kept in the clear so a human can tell
// two of them apart in a list.
const KeyPrefixLen = 8

// GenerateKey mints a key with 256 bits of entropy.
func GenerateKey() (GeneratedKey, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return GeneratedKey{}, fmt.Errorf("auth: generating key: %w", err)
	}
	secret := "rlk_" + base64.RawURLEncoding.EncodeToString(buf)
	sum := sha256.Sum256([]byte(secret))
	return GeneratedKey{
		Secret: secret,
		Hash:   sum[:],
		Prefix: secret[:KeyPrefixLen],
	}, nil
}

// HashKey is how a presented key is turned into the value the store holds.
func HashKey(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

// AdminToken guards the admin API.
type AdminToken struct {
	expected []byte
}

// NewAdminToken builds a comparator. An empty token disables the admin API,
// because a management surface with no credential is worse than no management
// surface.
func NewAdminToken(token string) *AdminToken {
	if token == "" {
		return &AdminToken{}
	}
	sum := sha256.Sum256([]byte(token))
	return &AdminToken{expected: sum[:]}
}

// Configured reports whether an admin token was set.
func (t *AdminToken) Configured() bool { return len(t.expected) > 0 }

// Check verifies the credential on an admin request in constant time.
func (t *AdminToken) Check(r *http.Request) error {
	if !t.Configured() {
		return errors.New("auth: the admin API is disabled because no admin token is configured")
	}
	presented := bearerFrom(r)
	if presented == "" {
		presented = r.Header.Get("X-Admin-Token")
	}
	if presented == "" {
		return ErrNoCredentials
	}
	sum := sha256.Sum256([]byte(presented))
	if subtle.ConstantTimeCompare(sum[:], t.expected) != 1 {
		return ErrBadCredentials
	}
	return nil
}

// SignTenantToken mints an HS256 token for a tenant. It exists for tests and
// for the admin API's "give me something I can curl with" endpoint; nothing on
// the request path calls it.
func SignTenantToken(secret []byte, tenant, issuer, audience, claim string, ttl time.Duration) (string, error) {
	if claim == "" {
		claim = "tid"
	}
	now := time.Now()
	claims := jwt.MapClaims{
		claim: tenant,
		"iat": now.Unix(),
		"exp": now.Add(ttl).Unix(),
	}
	if issuer != "" {
		claims["iss"] = issuer
	}
	if audience != "" {
		claims["aud"] = audience
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(secret)
	if err != nil {
		return "", fmt.Errorf("auth: signing: %w", err)
	}
	return signed, nil
}

// FingerprintSecret is a short, non-reversible identifier for a configured
// secret, so logs and the admin API can say WHICH secret is loaded without
// printing it.
func FingerprintSecret(secret []byte) string {
	if len(secret) == 0 {
		return "none"
	}
	sum := sha256.Sum256(secret)
	return hex.EncodeToString(sum[:4])
}
