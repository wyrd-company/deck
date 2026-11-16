package cli

import (
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"google.golang.org/protobuf/encoding/protojson"
	"testing"
)

func TestEventContract(t *testing.T) {
	a := ownActivity(keyA, "original", 0)
	raw, _ := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(a)
	if err := validateEvent(raw, &pb.Activity{}); err != nil {
		t.Fatalf("%s: %v", raw, err)
	}
}
