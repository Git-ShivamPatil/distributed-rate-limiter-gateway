package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type fakeKeys map[string]string // presented secret -> tenant

func (f fakeKeys) TenantForKey(_ context.Context, presented string) (string, error) {
	if tenant, ok := f[presented]; ok {
		return tenant, nil
	}
	return "", errors.New("unknown key")
}

func request(headers map[string]string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/v1/check", nil)
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	return r
}

func TestAPIKeyResolvesTenant(t *testing.T) {
	a := New(WithAPIKeys(fakeKeys{"rlk_secret": "acme"}))

	for _, headers := range []map[string]string{
		{"X-API-Key": "rlk_secret"},
		{"Authorization": "ApiKey rlk_secret"},
	} {
		tenant, err := a.Tenant(context.Background(), request(headers))
		if err != nil {
			t.Fatalf("%v: %v", headers, err)
		}
		if tenant != "acme" {
			t.Fatalf("%v: tenant = %q, want acme", headers, tenant)
		}
	}

	if _, err := a.Tenant(context.Background(), request(map[string]string{"X-API-Key": "wrong"})); !errors.Is(err, ErrBadCredentials) {
		t.Fatalf("an unknown key gave %v, want ErrBadCredentials", err)
	}
	if _, err := a.Tenant(context.Background(), request(nil)); !errors.Is(err, ErrNoCredentials) {
		t.Fatalf("no credentials gave %v, want ErrNoCredentials", err)
	}
}

func TestJWTResolvesTenant(t *testing.T) {
	secret := []byte("a-test-signing-secret")
	a := New(WithJWT(secret, "gateway-test", "limiter", "tid"))

	token, err := SignTenantToken(secret, "acme", "gateway-test", "limiter", "tid", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	tenant, err := a.Tenant(context.Background(), request(map[string]string{"Authorization": "Bearer " + token}))
	if err != nil {
		t.Fatal(err)
	}
	if tenant != "acme" {
		t.Fatalf("tenant = %q, want acme", tenant)
	}
}

func TestJWTRejectsTheUsualForgeries(t *testing.T) {
	secret := []byte("a-test-signing-secret")
	a := New(WithJWT(secret, "gateway-test", "limiter", "tid"))

	valid := func(claims jwt.MapClaims, method jwt.SigningMethod, key any) string {
		tok := jwt.NewWithClaims(method, claims)
		s, err := tok.SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	base := func() jwt.MapClaims {
		return jwt.MapClaims{
			"tid": "acme", "iss": "gateway-test", "aud": "limiter",
			"iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix(),
		}
	}

	cases := []struct {
		name  string
		token string
	}{
		{"signed with another key", valid(base(), jwt.SigningMethodHS256, []byte("not-the-secret"))},
		{"expired", valid(func() jwt.MapClaims {
			c := base()
			c["exp"] = time.Now().Add(-time.Minute).Unix()
			return c
		}(), jwt.SigningMethodHS256, secret)},
		{"wrong issuer", valid(func() jwt.MapClaims {
			c := base()
			c["iss"] = "somebody-else"
			return c
		}(), jwt.SigningMethodHS256, secret)},
		{"wrong audience", valid(func() jwt.MapClaims {
			c := base()
			c["aud"] = "another-service"
			return c
		}(), jwt.SigningMethodHS256, secret)},
		{"no tenant claim", valid(func() jwt.MapClaims {
			c := base()
			delete(c, "tid")
			return c
		}(), jwt.SigningMethodHS256, secret)},
		{"no expiry at all", valid(func() jwt.MapClaims {
			c := base()
			delete(c, "exp")
			return c
		}(), jwt.SigningMethodHS256, secret)},
		{"alg none", noneToken(t, base())},
		{"garbage", "not.a.token"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenant, err := a.Tenant(context.Background(),
				request(map[string]string{"Authorization": "Bearer " + tc.token}))
			if err == nil {
				t.Fatalf("accepted a bad token and resolved tenant %q", tenant)
			}
			if !errors.Is(err, ErrBadCredentials) {
				t.Fatalf("err = %v, want ErrBadCredentials", err)
			}
		})
	}
}

// noneToken hand-builds an unsigned token. The "alg: none" forgery is the
// reason the parser pins its accepted algorithms rather than trusting the
// header, so it is worth constructing rather than assuming.
func noneToken(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "none", "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	enc := base64.RawURLEncoding.EncodeToString
	return enc(header) + "." + enc(payload) + "."
}

// An API key wins over a bearer token, and a request carrying a bad key is
// refused rather than falling through to the token.
func TestAPIKeyTakesPrecedence(t *testing.T) {
	secret := []byte("a-test-signing-secret")
	a := New(WithAPIKeys(fakeKeys{"rlk_good": "acme"}), WithJWT(secret, "", "", "tid"))
	token, _ := SignTenantToken(secret, "globex", "", "", "tid", time.Hour)

	tenant, err := a.Tenant(context.Background(), request(map[string]string{
		"X-API-Key":     "rlk_good",
		"Authorization": "Bearer " + token,
	}))
	if err != nil || tenant != "acme" {
		t.Fatalf("tenant = %q err = %v, want acme", tenant, err)
	}

	if _, err := a.Tenant(context.Background(), request(map[string]string{
		"X-API-Key":     "rlk_bad",
		"Authorization": "Bearer " + token,
	})); !errors.Is(err, ErrBadCredentials) {
		t.Fatal("a bad API key fell through to the bearer token, so a wrong key is a way of becoming somebody else")
	}
}

func TestAuthenticatorWithNothingConfigured(t *testing.T) {
	a := New()
	if a.Enabled() {
		t.Fatal("an authenticator with no mechanisms reports itself enabled")
	}
	if _, err := a.Tenant(context.Background(), request(map[string]string{"X-API-Key": "anything"})); !errors.Is(err, ErrNoCredentials) {
		t.Fatalf("err = %v, want ErrNoCredentials", err)
	}
}

func TestGeneratedKeysAreDistinctAndHashed(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		key, err := GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		if seen[key.Secret] {
			t.Fatal("GenerateKey returned a duplicate")
		}
		seen[key.Secret] = true

		if !strings.HasPrefix(key.Secret, "rlk_") {
			t.Fatalf("key %q has no recognisable prefix", key.Secret)
		}
		if key.Prefix != key.Secret[:KeyPrefixLen] {
			t.Fatal("the stored prefix does not match the secret")
		}
		if strings.Contains(string(key.Hash), key.Secret) {
			t.Fatal("the hash contains the secret")
		}
		if got := HashKey(key.Secret); string(got) != string(key.Hash) {
			t.Fatal("HashKey does not reproduce the stored hash, so no presented key could ever match")
		}
	}
}

func TestAdminToken(t *testing.T) {
	t.Run("unset disables the admin API", func(t *testing.T) {
		tok := NewAdminToken("")
		if tok.Configured() {
			t.Fatal("an empty admin token reports itself configured")
		}
		if err := tok.Check(request(map[string]string{"X-Admin-Token": "anything"})); err == nil {
			t.Fatal("an unconfigured admin token accepted a credential")
		}
	})

	t.Run("accepts the right credential in either header", func(t *testing.T) {
		tok := NewAdminToken("s3cret")
		for _, headers := range []map[string]string{
			{"X-Admin-Token": "s3cret"},
			{"Authorization": "Bearer s3cret"},
		} {
			if err := tok.Check(request(headers)); err != nil {
				t.Fatalf("%v: %v", headers, err)
			}
		}
	})

	t.Run("rejects the wrong one", func(t *testing.T) {
		tok := NewAdminToken("s3cret")
		if err := tok.Check(request(map[string]string{"X-Admin-Token": "s3cret-but-longer"})); !errors.Is(err, ErrBadCredentials) {
			t.Fatalf("err = %v, want ErrBadCredentials", err)
		}
		if err := tok.Check(request(nil)); !errors.Is(err, ErrNoCredentials) {
			t.Fatalf("err = %v, want ErrNoCredentials", err)
		}
	})
}

func TestFingerprintSaysWhichSecretWithoutSayingIt(t *testing.T) {
	secret := []byte("a-test-signing-secret")
	fp := FingerprintSecret(secret)
	if strings.Contains(fp, "secret") || len(fp) != 8 {
		t.Fatalf("fingerprint %q is not a short opaque identifier", fp)
	}
	if FingerprintSecret([]byte("another")) == fp {
		t.Fatal("two different secrets share a fingerprint")
	}
	if FingerprintSecret(nil) != "none" {
		t.Fatal("an absent secret should be reported as none")
	}
}
