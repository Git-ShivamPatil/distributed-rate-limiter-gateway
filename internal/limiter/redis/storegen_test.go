package redis

import (
	"context"
	"sync"
	"testing"
)

// The generation is minted exactly once per lifetime of the store, whoever
// asks and however many ask at once. Redis runs the script to completion, so
// the first caller writes and every other one reads what it wrote -- no lock,
// no retry, no leader.
func TestTheStoreGenerationIsMintedOncePerLifetime(t *testing.T) {
	client := testClient(t)
	checker := New(client)
	ctx := context.Background()

	got, err := checker.MintStoreGeneration(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	if got != 7 {
		t.Fatalf("minting on an empty store returned %d, want 7", got)
	}

	// A second caller arriving with a different candidate gets the one that
	// is already in force, not its own.
	got, err = checker.MintStoreGeneration(ctx, 99)
	if err != nil {
		t.Fatal(err)
	}
	if got != 7 {
		t.Fatalf("a second mint returned %d; the generation is per lifetime, not per caller", got)
	}

	if _, err := checker.MintStoreGeneration(ctx, 0); err == nil {
		t.Fatal("generation 0 was accepted, and zero means unfenced everywhere else")
	}
}

func TestConcurrentMintersAllAgree(t *testing.T) {
	client := testClient(t)
	checker := New(client)

	const racers = 24
	var wg sync.WaitGroup
	results := make([]uint64, racers)
	errs := make([]error, racers)

	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Every gateway computes the same candidate from the same
			// committed generation, but they are deliberately different here:
			// whatever they propose, they must all leave agreeing.
			results[i], errs[i] = checker.MintStoreGeneration(context.Background(), uint64(i+1))
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("minter %d: %v", i, err)
		}
	}
	for i, got := range results {
		if got != results[0] {
			t.Fatalf("minter %d got %d and minter 0 got %d; the store has two names", i, got, results[0])
		}
	}
}

// THE REASON THE RUN ID CANNOT BE THE ONLY SIGNAL, against a real Redis.
//
// A flush empties every key and leaves the server process untouched, so its
// run id is exactly what it was. A detector watching only the run id would see
// a perfectly healthy store that had just handed every tenant a fresh quota.
func TestAFlushEmptiesTheStoreWithoutChangingItsRunId(t *testing.T) {
	client := testClient(t)
	checker := New(client)
	ctx := context.Background()

	if _, err := checker.MintStoreGeneration(ctx, 4); err != nil {
		t.Fatal(err)
	}
	gen, runBefore, err := checker.Identity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if gen != 4 {
		t.Fatalf("the store reports generation %d, want 4", gen)
	}
	if runBefore == "" {
		t.Fatal("the store reports no run id at all, so the restart signal would never fire")
	}

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	gen, runAfter, err := checker.Identity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if gen != 0 {
		t.Fatalf("the generation survived a flush as %d", gen)
	}
	if runAfter != runBefore {
		t.Skipf("this Redis changed its run id across a flush (%q then %q), so the point cannot be made here",
			runBefore, runAfter)
	}
	// This is the assertion: the store lost everything and still looks
	// identical by run id. Only the missing generation says otherwise.
	t.Logf("run id unchanged across a flush (%s); the missing generation is the only signal", runAfter)
}
