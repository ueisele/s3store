package s3store

import (
	"reflect"
	"testing"

	"github.com/ueisele/s3store/s3parquet"
)

type umbrellaTestRec struct {
	Customer string `parquet:"customer"`
}

// TestWriterKnobsMirroredInUmbrellaConfig guards the two-hop
// projection s3store.Config → s3parquet.Config → WriterConfig:
// every pure-write knob on s3parquet.WriterConfig (excluding
// Target, which is flattened on umbrella, and PartitionKeyOf,
// which is a typed func the umbrella already carries) must also
// appear on s3store.Config with the same name and type. Without
// this, adding a new write-side config knob and forgetting the
// umbrella would leave umbrella users unable to reach it.
func TestWriterKnobsMirroredInUmbrellaConfig(t *testing.T) {
	wc := reflect.TypeFor[s3parquet.WriterConfig[umbrellaTestRec]]()
	uc := reflect.TypeFor[Config[umbrellaTestRec]]()

	for i := range wc.NumField() {
		wf := wc.Field(i)
		switch wf.Name {
		case "Target", "PartitionKeyOf":
			continue
		}
		uf, ok := uc.FieldByName(wf.Name)
		if !ok {
			t.Errorf("WriterConfig field %q missing from s3store.Config",
				wf.Name)
			continue
		}
		if wf.Type != uf.Type {
			t.Errorf("WriterConfig.%s type %s != s3store.Config.%s type %s",
				wf.Name, wf.Type, uf.Name, uf.Type)
		}
	}
}
