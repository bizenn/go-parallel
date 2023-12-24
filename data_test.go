package parallel

import (
	"errors"
	"testing"
)

func TestEmptyData(t *testing.T) {
	errX := errors.New("X")
	for _, err := range []error{nil, errX} {
		d := NewData[any](nil, nil)
		if d.Value() != nil {
			t.Errorf("Value: %v expected but got %v", nil, d.Value())
		}
		if d.Err() != nil {
			t.Errorf("Err: %v expected but got %v", err, d.Err())
		}

		dS := NewData("", nil)
		if dS.Value() != "" {
			t.Errorf("Value: %v expected but got %v", "", dS.Value())
		}
		if dS.Err() != nil {
			t.Errorf("Err: %v expected but got %v", err, dS.Err())
		}

		dI := NewData(0, nil)
		if dI.Value() != 0 {
			t.Errorf("Value %v expected but got %v", 0, dI.Value())
		}
		if dI.Err() != nil {
			t.Errorf("Value %v expected but got %v", err, dI.Err())
		}

		type D struct{ V int }
		var ss D
		dSS := NewData(ss, nil)
		if dSS.Value() != ss {
			t.Errorf("Value %v expected but got %v", D{}, dSS.Value())
		}
		if dSS.Err() != nil {
			t.Errorf("Err %v expected but got %v", err, dSS.Err())
		}

		dSP := NewData[*D](nil, nil)
		if dSP.Value() != nil {
			t.Errorf("Value %v expected but got %v", nil, dSP.Value())
		}
		if dSP.Err() != nil {
			t.Errorf("Err %v expected but got %v", err, dSP.Err())
		}
	}
}

func TestValueData(t *testing.T) {
	v := struct{}{}
	d := NewData(v, nil)
	if d.Value() != v {
		t.Errorf("Value: %v expected but got %v", v, d.Value())
	}
	if d.Err() != nil {
		t.Errorf("Err: %v expected but got %v", nil, d.Err())
	}

	dS := NewData("string", nil)
	if dS.Value() != "string" {
		t.Errorf("Value: %v expected but got %v", "string", dS.Value())
	}
	if dS.Err() != nil {
		t.Errorf("Err: %v expected but got %v", nil, dS.Err())
	}

	dI := NewData(10, nil)
	if dI.Value() != 10 {
		t.Errorf("Value %v expected but got %v", 10, dI.Value())
	}
	if dI.Err() != nil {
		t.Errorf("Value %v expected but got %v", nil, dI.Err())
	}

	type D struct{ V int }
	ss := D{10}
	dSS := NewData(ss, nil)
	if dSS.Value() != ss {
		t.Errorf("Value %v expected but got %v", ss, dSS.Value())
	}
	if dSS.Err() != nil {
		t.Errorf("Err %v expected but got %v", nil, dSS.Err())
	}

	ssp := &ss
	dSP := NewData[*D](ssp, nil)
	if dSP.Value() != ssp {
		t.Errorf("Value %v expected but got %v", ssp, dSP.Value())
	}
	if dSP.Err() != nil {
		t.Errorf("Err %v expected but got %v", nil, dSP.Err())
	}
}
