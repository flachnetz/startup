package startup_tracing

import (
	"github.com/mailru/easyjson"
	"github.com/mailru/easyjson/buffer"
	"github.com/mailru/easyjson/jwriter"
	"github.com/openzipkin/zipkin-go/model"
	"net"
	"strings"
	"time"
)

//go:generate easyjson span-serializer.go

func init() {
	buffer.Init(buffer.PoolConfig{
		StartSize:  4 * 1024,
		PooledSize: 4 * 1024,
		MaxSize:    64 * 1024,
	})
}

// spanSerializer implements the default JSON encoding SpanSerializer.
type spanSerializer struct{}

// Serialize takes an array of Zipkin model.SpanModel objects and returns a JSON
// encoding of it.
func (spanSerializer) Serialize(spans []*model.SpanModel) ([]byte, error) {
	jsonSpans := make(jsonSpanSlice, len(spans))

	for idx, span := range spans {
		jsonSpans[idx] = mapSpan(span)
	}

	return easyjson.Marshal(jsonSpans)
}

func mapSpan(span *model.SpanModel) jsonSpanModel {
	jsonSpan := jsonSpanModel{
		TraceID:   jsonTraceId(span.TraceID),
		ID:        jsonId(span.ID),
		Debug:     span.Debug,
		Name:      strings.ToLower(span.Name),
		Kind:      string(span.Kind),
		Timestamp: (span.Timestamp.UnixNano() + 500) / 1e3,
		Duration:  durationInMicros(span.Duration),
		Shared:    span.Shared,
		Tags:      span.Tags,
	}

	if span.ParentID != nil {
		jsonSpan.ParentID = jsonId(*span.ParentID)
	}

	if ep := span.LocalEndpoint; ep != nil && !ep.Empty() {
		jsonSpan.LocalEndpoint = &jsonEndpoint{
			ServiceName: ep.ServiceName,
			IPv4:        jsonIpV4(ep.IPv4),
			Port:        ep.Port,
		}
	}

	if ep := span.RemoteEndpoint; ep != nil && !ep.Empty() {
		jsonSpan.RemoteEndpoint = &jsonEndpoint{
			ServiceName: ep.ServiceName,
			IPv4:        jsonIpV4(ep.IPv4),
			Port:        ep.Port,
		}
	}

	if len(span.Annotations) > 0 {
		jsonAnnotations := make([]jsonAnnotation, len(span.Annotations))

		for idx, ann := range span.Annotations {
			jsonAnnotations[idx] = jsonAnnotation{
				Timestamp: ann.Timestamp.Round(time.Microsecond).UnixNano() / 1e3,
				Value:     ann.Value,
			}
		}

		jsonSpan.Annotations = jsonAnnotations
	}

	return jsonSpan
}

func durationInMicros(duration time.Duration) int64 {
	if duration < 1*time.Microsecond {
		return 1
	}

	return int64((duration + 500*time.Nanosecond) / time.Microsecond)
}

// ContentType returns the ContentType needed for this encoding.
func (spanSerializer) ContentType() string {
	return "application/json"
}

//easyjson:json
type jsonSpanSlice []jsonSpanModel

//easyjson:json
type jsonSpanModel struct {
	TraceID        jsonTraceId       `json:"traceId"`
	ID             jsonId            `json:"id"`
	ParentID       jsonId            `json:"parentId,omitempty"`
	Debug          bool              `json:"debug,omitempty"`
	Name           string            `json:"name,omitempty"`
	Kind           string            `json:"kind,omitempty"`
	Shared         bool              `json:"shared,omitempty"`
	Timestamp      int64             `json:"timestamp,omitempty"`
	Duration       int64             `json:"duration,omitempty"`
	LocalEndpoint  *jsonEndpoint     `json:"localEndpoint,omitempty"`
	RemoteEndpoint *jsonEndpoint     `json:"remoteEndpoint,omitempty"`
	Annotations    []jsonAnnotation  `json:"annotations,omitempty"`
	Tags           map[string]string `json:"tags,omitempty"`
}

//easyjson:json
type jsonEndpoint struct {
	ServiceName string   `json:"serviceName,omitempty"`
	IPv4        jsonIpV4 `json:"ipv4,omitempty"`
	Port        uint16   `json:"port,omitempty"`
}

//easyjson:json
type jsonAnnotation struct {
	Timestamp int64  `json:"timestamp"`
	Value     string `json:"value"`
}

var hex = [256][2]byte{
	{'0', '0'}, {'0', '1'}, {'0', '2'}, {'0', '3'}, {'0', '4'}, {'0', '5'}, {'0', '6'}, {'0', '7'}, {'0', '8'}, {'0', '9'}, {'0', 'a'}, {'0', 'b'}, {'0', 'c'}, {'0', 'd'}, {'0', 'e'}, {'0', 'f'},
	{'1', '0'}, {'1', '1'}, {'1', '2'}, {'1', '3'}, {'1', '4'}, {'1', '5'}, {'1', '6'}, {'1', '7'}, {'1', '8'}, {'1', '9'}, {'1', 'a'}, {'1', 'b'}, {'1', 'c'}, {'1', 'd'}, {'1', 'e'}, {'1', 'f'},
	{'2', '0'}, {'2', '1'}, {'2', '2'}, {'2', '3'}, {'2', '4'}, {'2', '5'}, {'2', '6'}, {'2', '7'}, {'2', '8'}, {'2', '9'}, {'2', 'a'}, {'2', 'b'}, {'2', 'c'}, {'2', 'd'}, {'2', 'e'}, {'2', 'f'},
	{'3', '0'}, {'3', '1'}, {'3', '2'}, {'3', '3'}, {'3', '4'}, {'3', '5'}, {'3', '6'}, {'3', '7'}, {'3', '8'}, {'3', '9'}, {'3', 'a'}, {'3', 'b'}, {'3', 'c'}, {'3', 'd'}, {'3', 'e'}, {'3', 'f'},
	{'4', '0'}, {'4', '1'}, {'4', '2'}, {'4', '3'}, {'4', '4'}, {'4', '5'}, {'4', '6'}, {'4', '7'}, {'4', '8'}, {'4', '9'}, {'4', 'a'}, {'4', 'b'}, {'4', 'c'}, {'4', 'd'}, {'4', 'e'}, {'4', 'f'},
	{'5', '0'}, {'5', '1'}, {'5', '2'}, {'5', '3'}, {'5', '4'}, {'5', '5'}, {'5', '6'}, {'5', '7'}, {'5', '8'}, {'5', '9'}, {'5', 'a'}, {'5', 'b'}, {'5', 'c'}, {'5', 'd'}, {'5', 'e'}, {'5', 'f'},
	{'6', '0'}, {'6', '1'}, {'6', '2'}, {'6', '3'}, {'6', '4'}, {'6', '5'}, {'6', '6'}, {'6', '7'}, {'6', '8'}, {'6', '9'}, {'6', 'a'}, {'6', 'b'}, {'6', 'c'}, {'6', 'd'}, {'6', 'e'}, {'6', 'f'},
	{'7', '0'}, {'7', '1'}, {'7', '2'}, {'7', '3'}, {'7', '4'}, {'7', '5'}, {'7', '6'}, {'7', '7'}, {'7', '8'}, {'7', '9'}, {'7', 'a'}, {'7', 'b'}, {'7', 'c'}, {'7', 'd'}, {'7', 'e'}, {'7', 'f'},
	{'8', '0'}, {'8', '1'}, {'8', '2'}, {'8', '3'}, {'8', '4'}, {'8', '5'}, {'8', '6'}, {'8', '7'}, {'8', '8'}, {'8', '9'}, {'8', 'a'}, {'8', 'b'}, {'8', 'c'}, {'8', 'd'}, {'8', 'e'}, {'8', 'f'},
	{'9', '0'}, {'9', '1'}, {'9', '2'}, {'9', '3'}, {'9', '4'}, {'9', '5'}, {'9', '6'}, {'9', '7'}, {'9', '8'}, {'9', '9'}, {'9', 'a'}, {'9', 'b'}, {'9', 'c'}, {'9', 'd'}, {'9', 'e'}, {'9', 'f'},
	{'a', '0'}, {'a', '1'}, {'a', '2'}, {'a', '3'}, {'a', '4'}, {'a', '5'}, {'a', '6'}, {'a', '7'}, {'a', '8'}, {'a', '9'}, {'a', 'a'}, {'a', 'b'}, {'a', 'c'}, {'a', 'd'}, {'a', 'e'}, {'a', 'f'},
	{'b', '0'}, {'b', '1'}, {'b', '2'}, {'b', '3'}, {'b', '4'}, {'b', '5'}, {'b', '6'}, {'b', '7'}, {'b', '8'}, {'b', '9'}, {'b', 'a'}, {'b', 'b'}, {'b', 'c'}, {'b', 'd'}, {'b', 'e'}, {'b', 'f'},
	{'c', '0'}, {'c', '1'}, {'c', '2'}, {'c', '3'}, {'c', '4'}, {'c', '5'}, {'c', '6'}, {'c', '7'}, {'c', '8'}, {'c', '9'}, {'c', 'a'}, {'c', 'b'}, {'c', 'c'}, {'c', 'd'}, {'c', 'e'}, {'c', 'f'},
	{'d', '0'}, {'d', '1'}, {'d', '2'}, {'d', '3'}, {'d', '4'}, {'d', '5'}, {'d', '6'}, {'d', '7'}, {'d', '8'}, {'d', '9'}, {'d', 'a'}, {'d', 'b'}, {'d', 'c'}, {'d', 'd'}, {'d', 'e'}, {'d', 'f'},
	{'e', '0'}, {'e', '1'}, {'e', '2'}, {'e', '3'}, {'e', '4'}, {'e', '5'}, {'e', '6'}, {'e', '7'}, {'e', '8'}, {'e', '9'}, {'e', 'a'}, {'e', 'b'}, {'e', 'c'}, {'e', 'd'}, {'e', 'e'}, {'e', 'f'},
	{'f', '0'}, {'f', '1'}, {'f', '2'}, {'f', '3'}, {'f', '4'}, {'f', '5'}, {'f', '6'}, {'f', '7'}, {'f', '8'}, {'f', '9'}, {'f', 'a'}, {'f', 'b'}, {'f', 'c'}, {'f', 'd'}, {'f', 'e'}, {'f', 'f'},
}

func appendId(id uint64) [16]byte {
	var val [16]byte

	copy(val[0:2], hex[byte(id>>56)][:2])
	copy(val[2:4], hex[byte(id>>48)][:2])
	copy(val[4:6], hex[byte(id>>40)][:2])
	copy(val[6:8], hex[byte(id>>32)][:2])
	copy(val[8:10], hex[byte(id>>24)][:2])
	copy(val[10:12], hex[byte(id>>16)][:2])
	copy(val[12:14], hex[byte(id>>8)][:2])
	copy(val[14:16], hex[byte(id>>0)][:2])

	return val
}

type jsonId uint64

func (j jsonId) MarshalEasyJSON(w *jwriter.Writer) {
	buf := appendId(uint64(j))

	w.RawByte('"')
	w.Raw(buf[:16], nil)
	w.RawByte('"')
}

type jsonTraceId model.TraceID

func (j jsonTraceId) MarshalEasyJSON(w *jwriter.Writer) {
	if j.High == 0 {
		jsonId(j.Low).MarshalEasyJSON(w)
		return
	}

	high := appendId(j.High)
	low := appendId(j.Low)

	w.RawByte('"')
	w.Raw(high[:], nil)
	w.Raw(low[:], nil)
	w.RawByte('"')
}

type jsonIpV4 net.IP

var smallNumberStrings = [256][]byte{
	[]byte("0"), []byte("1"), []byte("2"), []byte("3"), []byte("4"), []byte("5"), []byte("6"), []byte("7"), []byte("8"), []byte("9"), []byte("10"), []byte("11"), []byte("12"), []byte("13"), []byte("14"), []byte("15"), []byte("16"), []byte("17"), []byte("18"), []byte("19"), []byte("20"), []byte("21"), []byte("22"), []byte("23"), []byte("24"), []byte("25"), []byte("26"), []byte("27"), []byte("28"), []byte("29"), []byte("30"), []byte("31"), []byte("32"), []byte("33"), []byte("34"), []byte("35"), []byte("36"), []byte("37"), []byte("38"), []byte("39"), []byte("40"), []byte("41"), []byte("42"), []byte("43"), []byte("44"), []byte("45"), []byte("46"), []byte("47"), []byte("48"), []byte("49"), []byte("50"), []byte("51"), []byte("52"), []byte("53"), []byte("54"), []byte("55"), []byte("56"), []byte("57"), []byte("58"), []byte("59"), []byte("60"), []byte("61"), []byte("62"), []byte("63"), []byte("64"), []byte("65"), []byte("66"), []byte("67"), []byte("68"), []byte("69"), []byte("70"), []byte("71"), []byte("72"), []byte("73"), []byte("74"), []byte("75"), []byte("76"), []byte("77"), []byte("78"), []byte("79"), []byte("80"), []byte("81"), []byte("82"), []byte("83"), []byte("84"), []byte("85"), []byte("86"), []byte("87"), []byte("88"), []byte("89"), []byte("90"), []byte("91"), []byte("92"), []byte("93"), []byte("94"), []byte("95"), []byte("96"), []byte("97"), []byte("98"), []byte("99"), []byte("100"), []byte("101"), []byte("102"), []byte("103"), []byte("104"), []byte("105"), []byte("106"), []byte("107"), []byte("108"), []byte("109"), []byte("110"), []byte("111"), []byte("112"), []byte("113"), []byte("114"), []byte("115"), []byte("116"), []byte("117"), []byte("118"), []byte("119"), []byte("120"), []byte("121"), []byte("122"), []byte("123"), []byte("124"), []byte("125"), []byte("126"), []byte("127"), []byte("128"), []byte("129"), []byte("130"), []byte("131"), []byte("132"), []byte("133"), []byte("134"), []byte("135"), []byte("136"), []byte("137"), []byte("138"), []byte("139"), []byte("140"), []byte("141"), []byte("142"), []byte("143"), []byte("144"), []byte("145"), []byte("146"), []byte("147"), []byte("148"), []byte("149"), []byte("150"), []byte("151"), []byte("152"), []byte("153"), []byte("154"), []byte("155"), []byte("156"), []byte("157"), []byte("158"), []byte("159"), []byte("160"), []byte("161"), []byte("162"), []byte("163"), []byte("164"), []byte("165"), []byte("166"), []byte("167"), []byte("168"), []byte("169"), []byte("170"), []byte("171"), []byte("172"), []byte("173"), []byte("174"), []byte("175"), []byte("176"), []byte("177"), []byte("178"), []byte("179"), []byte("180"), []byte("181"), []byte("182"), []byte("183"), []byte("184"), []byte("185"), []byte("186"), []byte("187"), []byte("188"), []byte("189"), []byte("190"), []byte("191"), []byte("192"), []byte("193"), []byte("194"), []byte("195"), []byte("196"), []byte("197"), []byte("198"), []byte("199"), []byte("200"), []byte("201"), []byte("202"), []byte("203"), []byte("204"), []byte("205"), []byte("206"), []byte("207"), []byte("208"), []byte("209"), []byte("210"), []byte("211"), []byte("212"), []byte("213"), []byte("214"), []byte("215"), []byte("216"), []byte("217"), []byte("218"), []byte("219"), []byte("220"), []byte("221"), []byte("222"), []byte("223"), []byte("224"), []byte("225"), []byte("226"), []byte("227"), []byte("228"), []byte("229"), []byte("230"), []byte("231"), []byte("232"), []byte("233"), []byte("234"), []byte("235"), []byte("236"), []byte("237"), []byte("238"), []byte("239"), []byte("240"), []byte("241"), []byte("242"), []byte("243"), []byte("244"), []byte("245"), []byte("246"), []byte("247"), []byte("248"), []byte("249"), []byte("250"), []byte("251"), []byte("252"), []byte("253"), []byte("254"), []byte("255"),
}

func (j jsonIpV4) MarshalEasyJSON(w *jwriter.Writer) {
	n := len(j)

	a := smallNumberStrings[j[n-4]]
	b := smallNumberStrings[j[n-3]]
	c := smallNumberStrings[j[n-2]]
	d := smallNumberStrings[j[n-1]]

	var space [256]byte

	pos := byte(0)

	space[0] = '"'
	copy(space[1:], a)
	pos += byte(len(a)) + 1

	space[pos] = '.'
	copy(space[pos+1:], b)
	pos += byte(len(b)) + 1

	space[pos] = '.'
	copy(space[pos+1:], c)
	pos += byte(len(c)) + 1

	space[pos] = '.'
	copy(space[pos+1:], d)
	pos += byte(len(d)) + 1

	space[pos] = '"'
	pos += 1

	w.Raw(space[:pos], nil)
}
