package common

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"

	alitsdb_serialization "github.com/caict-benchmark/BDC-TS/alitsdb_serializaition"
)

//DateTimeStdFormat the standard string format for date time
const DateTimeStdFormat = "2006-01-02 15:04:05.000"

type SerializerAliTSDBHttp struct {
}

type SerializerAliTSDB struct {
}

func NewSerializerAliTSDBHttp() *SerializerAliTSDBHttp {
	return &SerializerAliTSDBHttp{}
}

func NewSerializerAliTSDB() *SerializerAliTSDB {
	return &SerializerAliTSDB{}
}

// This function writes JSON lines that looks like:
// { <metric>, <timestamp>, <fields>, <tags> }
//
func (m *SerializerAliTSDBHttp) SerializePoint(w io.Writer, p *Point) (err error) {
	type wirePoint struct {
		Metric    string             `json:"metric"`
		Timestamp int64              `json:"timestamp"`
		Tags      map[string]string  `json:"tags"`
		Fields    map[string]float64 `json:"fields"`
	}

	encoder := json.NewEncoder(w)

	wp := wirePoint{}
	// Timestamps in AliTSDB must be millisecond precision:
	wp.Timestamp = p.Timestamp.UTC().UnixNano() / 1e6
	// sanity check
	{
		x := fmt.Sprintf("%d", wp.Timestamp)
		if len(x) != 13 {
			panic("serialized timestamp was not 13 digits")
		}
	}
	wp.Tags = make(map[string]string, len(p.TagKeys))
	for i := 0; i < len(p.TagKeys); i++ {
		// so many allocs..
		key := string(p.TagKeys[i])
		val := string(p.TagValues[i])
		wp.Tags[key] = val
	}
	// metric name
	wp.Metric = string(p.MeasurementName)

	// fields allocation
	wp.Fields = make(map[string]float64, len(p.FieldKeys))

	// for each Value, generate a new line in the output:
	for i := 0; i < len(p.FieldKeys); i++ {
		switch x := p.FieldValues[i].(type) {
		case int:
			wp.Fields[string(p.FieldKeys[i])] = float64(x)
		case int64:
			wp.Fields[string(p.FieldKeys[i])] = float64(x)
		case float32:
			wp.Fields[string(p.FieldKeys[i])] = float64(x)
		case float64:
			wp.Fields[string(p.FieldKeys[i])] = float64(x)
		default:
			panic("bad numeric value for AliTSDB serialization")
		}
	}
	err = encoder.Encode(wp)
	if err != nil {
		return err
	}

	return nil
}

func (s *SerializerAliTSDBHttp) SerializeSize(w io.Writer, points int64, values int64) error {
	//return serializeSizeInText(w, points, values)
	return nil
}

func (m *SerializerAliTSDB) SerializePoint(w io.Writer, p *Point) (err error) {
	var wp alitsdb_serialization.MultifieldPoint
	// metric
	wp.Metric = string(p.MeasurementName)

	// Timestamps in AliTSDB must be millisecond precision:
	wp.Timestamp = p.Timestamp.UTC().UnixNano() / 1e6
	// sanity check
	{
		x := fmt.Sprintf("%d", wp.Timestamp)
		if len(x) != 13 {
			panic("serialized timestamp was not 13 digits")
		}
	}

	// tags allocation
	wp.Tags = make(map[string]string, len(p.TagKeys))
	for i := 0; i < len(p.TagKeys); i++ {
		// so many allocs..
		key := string(p.TagKeys[i])
		val := string(p.TagValues[i])
		wp.Tags[key] = val
	}

	// fields allocation
	wp.Fields = make(map[string]float64, len(p.FieldKeys))

	// for each Value, generate a new line in the output:
	for i := 0; i < len(p.FieldKeys); i++ {
		switch x := p.FieldValues[i].(type) {
		case int:
			wp.Fields[string(p.FieldKeys[i])] = float64(x)
		case int64:
			wp.Fields[string(p.FieldKeys[i])] = float64(x)
		case float32:
			wp.Fields[string(p.FieldKeys[i])] = float64(x)
		case float64:
			wp.Fields[string(p.FieldKeys[i])] = float64(x)
		default:
			panic("bad numeric value for AliTSDB serialization")
		}
	}

	// write to the out stream
	out, err := wp.Marshal()
	if err != nil {
		log.Fatal(err)
	}
	s := uint64(len(out))
	binary.Write(w, binary.LittleEndian, s)
	w.Write(out)

	return nil
}

func (s *SerializerAliTSDB) SerializeSize(w io.Writer, points int64, values int64) error {
	//return serializeSizeInText(w, points, values)
	return nil
}
