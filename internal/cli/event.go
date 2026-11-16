package cli

import (
	"encoding/json"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
	deck "github.com/wyrd-company/deck"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"google.golang.org/protobuf/encoding/protojson"
)

var eventSchema = sync.OnceValues(func() (*jsonschema.Schema, error) {
	var value any
	if err := json.Unmarshal(deck.ActivitySchema, &value); err != nil {
		return nil, err
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource("activity.json", value); err != nil {
		return nil, err
	}
	return c.Compile("activity.json")
})

func validateEvent(raw []byte, event *pb.Activity) error {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	s, err := eventSchema()
	if err != nil {
		return err
	}
	if err = s.Validate(value); err != nil {
		return err
	}
	return protojson.Unmarshal(raw, event)
}
