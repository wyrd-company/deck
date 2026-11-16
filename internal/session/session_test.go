package session

import (
	"reflect"
	"testing"
)

func TestDetect(t *testing.T) {
	t.Setenv("DECK_TEST_SECOND", "2")
	t.Setenv("DECK_TEST_FIRST", "1")
	t.Setenv("DECK_TEST_EMPTY", "")
	t.Setenv("UNCONFIGURED", "ignored")
	got, err := Detect([]string{"DECK_TEST_FIRST", "DECK_TEST_EMPTY", "DECK_TEST_SECOND"}, "")
	if err != nil || got.Key() != "DECK_TEST_FIRST=1" || !reflect.DeepEqual(got.Identifiers, []Identifier{{"DECK_TEST_FIRST", "1"}, {"DECK_TEST_SECOND", "2"}}) {
		t.Fatalf("%+v %v", got, err)
	}
	got, err = Detect(nil, "")
	if err != nil || got.Key() != "" || len(got.Identifiers) != 0 {
		t.Fatalf("default %+v %v", got, err)
	}
	got, err = Detect(nil, "relay=a=b")
	if err != nil || got.Key() != "relay=a=b" {
		t.Fatalf("override %+v %v", got, err)
	}
	for _, bad := range []string{"oops", "=value", "name="} {
		if _, err = Detect(nil, bad); err == nil {
			t.Fatal(bad)
		}
	}
}
