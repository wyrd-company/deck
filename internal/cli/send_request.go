package cli

import (
	"fmt"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// The wire limit is specified by grpc-api.yml and enforced by the transport.
const maxSendRequestSize = 64 << 20

// Validate the actual request before recording a send: a local encoding or size
// failure is a usage error, with no possible remote outcome to reconcile.
func validateSendRequest(request proto.Message) error {
	if size := proto.Size(request); size > maxSendRequestSize {
		return invalid("%s: encoded request is %d bytes, exceeding 64 MiB; reduce this field", largestRequestField(request.ProtoReflect()), size)
	}
	if _, err := proto.Marshal(request); err != nil {
		if field := invalidUTF8Field(request.ProtoReflect(), ""); field != "" {
			return invalid("%s must contain valid UTF-8; convert text input to UTF-8 before sending", field)
		}
		return invalid("request cannot be encoded: %v", err)
	}
	return nil
}

// Name the largest contributing field rather than printing any message content.
func largestRequestField(message protoreflect.Message) string {
	var largest protoreflect.FieldDescriptor
	maxSize := -1
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		only := message.New()
		only.Set(field, value)
		if size := proto.Size(only.Interface()); size > maxSize {
			largest, maxSize = field, size
		}
		return true
	})
	if largest == nil {
		return "request"
	}
	path := string(largest.Name())
	if !largest.IsList() && !largest.IsMap() && largest.Kind() == protoreflect.MessageKind {
		path += "." + largestRequestField(message.Get(largest).Message())
	}
	return path
}

func invalidUTF8Field(message protoreflect.Message, prefix string) string {
	var bad string
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		path := prefix + string(field.Name())
		check := func(value protoreflect.Value, path string) {
			switch field.Kind() {
			case protoreflect.StringKind:
				if !utf8.ValidString(value.String()) {
					bad = path
				}
			case protoreflect.MessageKind:
				bad = invalidUTF8Field(value.Message(), path+".")
			}
		}
		switch {
		case field.IsMap():
			value.Map().Range(func(key protoreflect.MapKey, entry protoreflect.Value) bool {
				if field.MapKey().Kind() == protoreflect.StringKind && !utf8.ValidString(key.String()) {
					bad = path + " key"
				} else if field.MapValue().Kind() == protoreflect.MessageKind {
					bad = invalidUTF8Field(entry.Message(), path+".")
				} else if field.MapValue().Kind() == protoreflect.StringKind && !utf8.ValidString(entry.String()) {
					bad = path
				}
				return bad == ""
			})
		case field.IsList():
			for n := 0; n < value.List().Len() && bad == ""; n++ {
				check(value.List().Get(n), fmt.Sprintf("%s[%d]", path, n))
			}
		default:
			check(value, path)
		}
		return bad == ""
	})
	return bad
}
