package scheduler

import "testing"

func TestAllTokenClassesIncludesOverflow(t *testing.T) {
	classes := AllTokenClasses()
	if len(classes) != 5 {
		t.Fatalf("expected 5 token classes, got %d", len(classes))
	}
	if classes[4] != TokenClassOverflow {
		t.Fatalf("expected overflow token class to be present, got %#v", classes)
	}
}
