package envelope

import (
	"bytes"
	"errors"
	"maps"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/types"

	"github.com/norlis/event-driven/v2/pkg/event"
)

var defaults = Defaults{ID: "broker-1", Type: "io.test.message", Source: "//test"}

func sample() cloudevents.Event {
	ce := cloudevents.New()
	ce.SetID("id-1")
	ce.SetSource("/orders")
	ce.SetType("order.created")
	ce.SetSubject("42")
	ce.SetTime(time.Date(2026, 10, 21, 10, 30, 15, 123456789, time.FixedZone("CET", 3600)))
	ce.SetExtension("correlationid", "c-1")
	ce.SetExtension("retrycount", int32(3))
	ce.SetExtension("replayed", true)
	ce.SetExtension("schema", types.URI{Scheme: "https", Host: "example.com", Path: "/s"})
	_ = ce.SetData(cloudevents.ApplicationJSON, []byte(`{"k":1}`))
	return ce
}

func TestEncodeMapsContextAttributesAndExtensions(t *testing.T) {
	t.Parallel()

	got, err := Encode(sample())
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	want := map[string]string{
		"ce-id":            "id-1",
		"ce-specversion":   "1.0",
		"ce-type":          "order.created",
		"ce-source":        "/orders",
		"ce-subject":       "42",
		"ce-time":          "2026-10-21T10:30:15.123456789+01:00",
		"ce-correlationid": "c-1",
		"ce-retrycount":    "3",
		"ce-replayed":      "true",
		"ce-schema":        "https://example.com/s",
	}
	if !maps.Equal(got, want) {
		t.Errorf("Encode() = %v, want %v", got, want)
	}
}

func TestEncodeOmitsEmptySubjectAndZeroTime(t *testing.T) {
	t.Parallel()

	ce := cloudevents.New()
	ce.SetID("a")
	ce.SetSource("/s")
	ce.SetType("t")
	got, err := Encode(ce)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	for _, k := range []string{"ce-subject", "ce-time"} {
		if _, ok := got[k]; ok {
			t.Errorf("Encode() emitted %s for an event without it", k)
		}
	}
	if len(got) != 4 {
		t.Errorf("Encode() emitted %d attributes, want 4", len(got))
	}
}

func TestContentType(t *testing.T) {
	t.Parallel()

	ce := cloudevents.New()
	if got := ContentType(ce); got != cloudevents.ApplicationJSON {
		t.Errorf("ContentType(empty) = %q, want %q", got, cloudevents.ApplicationJSON)
	}
	ce.SetDataContentType("application/avro")
	if got := ContentType(ce); got != "application/avro" {
		t.Errorf("ContentType() = %q, want %q", got, "application/avro")
	}
}

func TestRoundTripPreservesSubSeconds(t *testing.T) {
	t.Parallel()

	src := sample()
	attrs, err := Encode(src)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(ContentType(src), maps.All(attrs), src.Data(), defaults)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if !got.Time().Equal(src.Time()) {
		t.Errorf("Time = %s, want %s", got.Time().Format(time.RFC3339Nano), src.Time().Format(time.RFC3339Nano))
	}
	if got.ID() != "id-1" || got.Type() != "order.created" || got.Source() != "/orders" || got.Subject() != "42" {
		t.Errorf("context attributes lost: %+v", got.Context)
	}
	if got.DataContentType() != cloudevents.ApplicationJSON {
		t.Errorf("DataContentType = %q, want %q", got.DataContentType(), cloudevents.ApplicationJSON)
	}
	if string(got.Data()) != `{"k":1}` {
		t.Errorf("Data = %s", got.Data())
	}
	// Extensions come back as strings: binary mode has no type information.
	wantExt := map[string]any{"correlationid": "c-1", "retrycount": "3", "replayed": "true", "schema": "https://example.com/s"}
	if !maps.Equal(got.Extensions(), wantExt) {
		t.Errorf("Extensions = %v, want %v", got.Extensions(), wantExt)
	}
}

func TestDecodeAppliesDefaults(t *testing.T) {
	t.Parallel()

	got, err := Decode("", maps.All(map[string]string{}), []byte(`{}`), defaults)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got.ID() != "broker-1" || got.Type() != "io.test.message" || got.Source() != "//test" {
		t.Errorf("defaults not applied: id=%q type=%q source=%q", got.ID(), got.Type(), got.Source())
	}
	if got.SpecVersion() != "1.0" {
		t.Errorf("SpecVersion = %q, want 1.0", got.SpecVersion())
	}
	if !got.Time().IsZero() {
		t.Errorf("Time = %s, want zero when ce-time is absent", got.Time())
	}
	if got.DataContentType() != cloudevents.ApplicationJSON {
		t.Errorf("DataContentType = %q, want application/json when Content-Type is empty", got.DataContentType())
	}
}

func TestDecodeIgnoresUnprefixedAndNormalizesCase(t *testing.T) {
	t.Parallel()

	attrs := map[string]string{
		"ce-id":            "x",
		"Ce-Correlationid": "c-2",
		"Content-Type":     "application/json",
		"googclient_x":     "ignored",
	}
	got, err := Decode(attrs["Content-Type"], maps.All(attrs), nil, defaults)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	wantExt := map[string]any{"correlationid": "c-2"}
	if !maps.Equal(got.Extensions(), wantExt) {
		t.Errorf("Extensions = %v, want %v", got.Extensions(), wantExt)
	}
}

func TestDecodeStructured(t *testing.T) {
	t.Parallel()

	body := []byte(`{"specversion":"1.0","id":"abc","source":"/s","type":"t","data":{"k":1}}`)
	got, err := Decode(cloudevents.ApplicationCloudEventsJSON, maps.All(map[string]string{}), body, defaults)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got.ID() != "abc" {
		t.Errorf("ID = %q, want abc", got.ID())
	}
	if string(got.Data()) != `{"k":1}` {
		t.Errorf("Data = %s, want {\"k\":1}", got.Data())
	}
}

func TestDecodeErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		attrs       map[string]string
		data        []byte
	}{
		{name: "structured duplicate member", contentType: cloudevents.ApplicationCloudEventsJSON, data: []byte(`{"specversion":"1.0","id":"a","id":"b","source":"/s","type":"t"}`)},
		{name: "structured invalid json", contentType: cloudevents.ApplicationCloudEventsJSON, data: []byte(`{`)},
		{name: "invalid ce-time", attrs: map[string]string{"ce-time": "yesterday"}},
		{name: "invalid ce-source fails Validate", attrs: map[string]string{"ce-source": "::not a uri"}},
		{name: "structured missing required attributes", contentType: cloudevents.ApplicationCloudEventsJSON, data: []byte(`{"specversion":"1.0","data":{"k":1}}`)},
		{name: "duplicate attribute differing in case", attrs: map[string]string{"ce-id": "a", "Ce-Id": "b"}},
		{name: "duplicate extension differing in case", attrs: map[string]string{"ce-correlationid": "a", "CE-CORRELATIONID": "b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := Decode(tt.contentType, maps.All(tt.attrs), tt.data, defaults)
			if !errors.Is(err, event.ErrDecode) {
				t.Fatalf("Decode() error = %v, want errors.Is(err, event.ErrDecode)", err)
			}
		})
	}
}

// dataschema is a spec context attribute, not an extension: official SDK
// producers send it as ce-dataschema and it must round-trip, not dead-letter.
func TestRoundTripDataSchema(t *testing.T) {
	t.Parallel()

	src := cloudevents.New()
	src.SetID("a")
	src.SetSource("/s")
	src.SetType("t")
	src.SetDataSchema("https://example.com/schema.json")
	attrs, err := Encode(src)
	if err != nil {
		t.Fatal(err)
	}
	if attrs["ce-dataschema"] != "https://example.com/schema.json" {
		t.Errorf("ce-dataschema = %q", attrs["ce-dataschema"])
	}
	got, err := Decode("", maps.All(attrs), nil, defaults)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got.DataSchema() != "https://example.com/schema.json" {
		t.Errorf("DataSchema = %q", got.DataSchema())
	}
	if len(got.Extensions()) != 0 {
		t.Errorf("Extensions = %v, want none", got.Extensions())
	}
}

// The transport Content-Type is authoritative; a ce-datacontenttype attribute
// from another binding is ignored instead of poisoning the event.
func TestDecodeIgnoresDataContentTypeAttribute(t *testing.T) {
	t.Parallel()

	got, err := Decode("application/avro", maps.All(map[string]string{"ce-datacontenttype": "text/plain"}), []byte("x"), defaults)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got.DataContentType() != "application/avro" {
		t.Errorf("DataContentType = %q, want application/avro", got.DataContentType())
	}
}

// assertSameEvent compares what structured mode must preserve. Extensions are
// compared formatted: the JSON format carries them without their Go type.
func assertSameEvent(t *testing.T, got, want cloudevents.Event) {
	t.Helper()
	if got.ID() != want.ID() || got.Type() != want.Type() || got.Source() != want.Source() || got.Subject() != want.Subject() {
		t.Errorf("context = %s/%s/%s/%s, want %s/%s/%s/%s",
			got.ID(), got.Type(), got.Source(), got.Subject(), want.ID(), want.Type(), want.Source(), want.Subject())
	}
	if !got.Time().Equal(want.Time()) {
		t.Errorf("Time = %v, want %v", got.Time(), want.Time())
	}
	if got.DataContentType() != want.DataContentType() {
		t.Errorf("DataContentType = %q, want %q", got.DataContentType(), want.DataContentType())
	}
	if !bytes.Equal(got.Data(), want.Data()) {
		t.Errorf("Data = %q, want %q", got.Data(), want.Data())
	}
	for name, v := range want.Extensions() {
		wantS, _ := types.Format(v)
		gotV, ok := got.Extensions()[name]
		if !ok {
			t.Errorf("extension %s missing", name)
			continue
		}
		if gotS, _ := types.Format(gotV); gotS != wantS {
			t.Errorf("extension %s = %q, want %q", name, gotS, wantS)
		}
	}
}

func TestEncodeStructuredRoundTrip(t *testing.T) {
	t.Parallel()

	ce := sample()
	ce.SetExtension("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")

	body, err := EncodeStructured(ce)
	if err != nil {
		t.Fatalf("EncodeStructured() error = %v", err)
	}
	got, err := Decode(cloudevents.ApplicationCloudEventsJSON, nil, body, Defaults{})
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	assertSameEvent(t, got, ce)
}

func TestEncodeStructuredRoundTripsNonJSONData(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		contentType string
		data        []byte
	}{
		{contentType: "text/plain", data: []byte("hello\nworld")},
		{contentType: "application/octet-stream", data: []byte{0x00, 0xff, 0x10}},
	} {
		t.Run(tc.contentType, func(t *testing.T) {
			t.Parallel()
			ce := sample()
			if err := ce.SetData(tc.contentType, tc.data); err != nil {
				t.Fatal(err)
			}
			body, err := EncodeStructured(ce)
			if err != nil {
				t.Fatalf("EncodeStructured() error = %v", err)
			}
			got, err := Decode(cloudevents.ApplicationCloudEventsJSON, nil, body, Defaults{})
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			assertSameEvent(t, got, ce)
		})
	}
}

// TestEncodeStructuredInlinesJSONData pins the body shape: valid JSON data is
// written as "data" (readable in a queue console, no base64 growth); anything
// else, including invalid JSON from a decode-failure dead-letter copy, stays
// in "data_base64" so the envelope remains parseable.
func TestEncodeStructuredInlinesJSONData(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		contentType string
		data        []byte
		want        string
	}{
		{name: "json", contentType: cloudevents.ApplicationJSON, data: []byte(`{"k":1}`), want: `"data":{"k":1}`},
		{name: "text json", contentType: cloudevents.TextJSON, data: []byte(`[1,2]`), want: `"data":[1,2]`},
		{name: "json suffix keeps base64", contentType: "application/vnd.order+json", data: []byte(`[1,2]`), want: `"data_base64":"WzEsMl0="`},
		{name: "invalid json", contentType: cloudevents.ApplicationJSON, data: []byte(`{"k":`), want: `"data_base64":"eyJrIjo="`},
		{name: "binary", contentType: "application/octet-stream", data: []byte{0x00, 0xff}, want: `"data_base64":"AP8="`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ce := sample()
			if err := ce.SetData(tc.contentType, tc.data); err != nil {
				t.Fatal(err)
			}
			body, err := EncodeStructured(ce)
			if err != nil {
				t.Fatalf("EncodeStructured() error = %v", err)
			}
			if !bytes.Contains(body, []byte(tc.want)) {
				t.Errorf("body %s does not contain %s", body, tc.want)
			}
			got, err := Decode(cloudevents.ApplicationCloudEventsJSON, nil, body, Defaults{})
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if !bytes.Equal(got.Data(), tc.data) {
				t.Errorf("round-trip Data = %q, want %q", got.Data(), tc.data)
			}
		})
	}
}

// Rescue must always produce an event the router can log, dead-letter and
// settle, whatever the transport handed it.
func TestRescueAlwaysValidates(t *testing.T) {
	t.Parallel()

	def := Defaults{ID: "broker-1", Type: "com.test.message", Source: "//test"}
	for _, tt := range []struct {
		name        string
		contentType string
		data        []byte
		wantCT      string
	}{
		{name: "invalid json", contentType: cloudevents.ApplicationJSON, data: []byte(`{"k":`), wantCT: cloudevents.ApplicationJSON},
		{name: "broken structured envelope", contentType: cloudevents.ApplicationCloudEventsJSON, data: []byte(`not an envelope`), wantCT: cloudevents.ApplicationCloudEventsJSON},
		{name: "empty content type and data", contentType: "", data: nil, wantCT: cloudevents.ApplicationJSON},
		{name: "binary", contentType: "application/octet-stream", data: []byte{0xff, 0x00}, wantCT: "application/octet-stream"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ce := Rescue(def, tt.contentType, tt.data)
			if err := ce.Validate(); err != nil {
				t.Fatalf("Validate() = %v", err)
			}
			if ce.ID() != def.ID || ce.Type() != def.Type || ce.Source() != def.Source {
				t.Errorf("attributes = %s/%s/%s, want defaults", ce.ID(), ce.Type(), ce.Source())
			}
			if ce.DataContentType() != tt.wantCT || !bytes.Equal(ce.Data(), tt.data) {
				t.Errorf("data = %q (%s), want %q (%s)", ce.Data(), ce.DataContentType(), tt.data, tt.wantCT)
			}
			if _, err := EncodeStructured(ce); err != nil {
				t.Errorf("a dead-letter copy of the rescue cannot be encoded: %v", err)
			}
		})
	}
}
