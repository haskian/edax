package kernel

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"strings"
	"testing"
)

// decode reads a vector's wire as its kind, keeping the fields the vectors pin.
func decode(kind, format string, wire []byte) (decodedMessage, error) {
	if kind == "event" {
		event, err := DecodeEvent(format, wire)
		if err != nil {
			return decodedMessage{}, err
		}
		return decodedMessage{ID: event.ID, Metadata: event.Metadata}, nil
	}
	command, err := DecodeCommand(format, wire)
	if err != nil {
		return decodedMessage{}, err
	}
	return decodedMessage{ID: command.ID, Metadata: command.Metadata}, nil
}

type decodedMessage struct {
	ID       string            `json:"id"`
	Metadata map[string]string `json:"metadata"`
}

// The wire is a trust boundary: Go decodes each vector in both formats to the kernel's message or its
// exact refusal. A NUL inside the wire never reaches Go's native string guard, so the kernel's rule is
// the only one that refuses it here.
func TestEnvelopeDecodeGolden(t *testing.T) {
	for _, row := range goldenRows[struct {
		Name       string `json:"name"`
		Kind       string `json:"kind"`
		JSONHex    string `json:"json_hex"`
		MsgpackHex string `json:"msgpack_hex"`
		Want       struct {
			Message *decodedMessage `json:"message"`
			Error   *string         `json:"error"`
		} `json:"want"`
	}](t, "envelope_decode") {
		for format, wireHex := range map[string]string{"json": row.JSONHex, "msgpack": row.MsgpackHex} {
			wire, err := hex.DecodeString(wireHex)
			if err != nil {
				t.Fatal(err)
			}
			got, err := decode(row.Kind, format, wire)
			if row.Want.Error != nil {
				// The vector holds the JSON refusal; MessagePack refuses with the same code, its own text.
				want := *row.Want.Error
				if format == "msgpack" {
					want, _, _ = strings.Cut(want, ":")
				}
				if err == nil || (err.Error() != want && !strings.HasPrefix(err.Error(), want+":")) {
					t.Fatalf("%s %s: error %v, want %q", row.Name, format, err, want)
				}
				continue
			}
			if err != nil {
				t.Fatalf("%s %s: %v", row.Name, format, err)
			}
			if got.ID != row.Want.Message.ID || !maps.Equal(got.Metadata, row.Want.Message.Metadata) {
				t.Fatalf("%s %s: got %+v, want %+v", row.Name, format, got, *row.Want.Message)
			}
		}
	}
}

// The document rule a Go host's own projection store calls: the check, and the keyed read.
func TestProjectionDocumentGolden(t *testing.T) {
	for _, row := range goldenRows[struct {
		Name string `json:"name"`
		ID   string `json:"id"`
		JSON string `json:"json"`
		Want struct {
			Keyed map[string]any `json:"keyed"`
			Error *string        `json:"error"`
		} `json:"want"`
	}](t, "projection_document") {
		checkErr := CheckProjectionDocument(row.ID, row.JSON)
		keyed, keyedErr := KeyedProjectionDocument(row.ID, row.JSON)
		if row.Want.Error != nil {
			for _, err := range []error{checkErr, keyedErr} {
				// Go's native string guard refuses a NUL argument first, with the same code.
				var kernelErr *Error
				nulArgument := strings.ContainsRune(row.ID, 0) && errors.As(err, &kernelErr) && kernelErr.Code == CodeCodec
				if !nulArgument && (err == nil || err.Error() != *row.Want.Error) {
					t.Fatalf("%s: error %v, want %q", row.Name, err, *row.Want.Error)
				}
			}
			continue
		}
		if checkErr != nil || keyedErr != nil {
			t.Fatalf("%s: %v, %v", row.Name, checkErr, keyedErr)
		}
		var got map[string]any
		if err := json.Unmarshal([]byte(keyed), &got); err != nil {
			t.Fatal(err)
		}
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(row.Want.Keyed)
		if string(gotJSON) != string(wantJSON) {
			t.Fatalf("%s: keyed %s, want %s", row.Name, gotJSON, wantJSON)
		}
	}
}
