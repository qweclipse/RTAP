import json
import math
import pytest
from rtap.events import new_event, encode_event, validate_event, alert_state


def test_json_roundtrip_and_nullable_fields():
    event = new_event("PC_camera", 1, 2, "person")
    assert json.loads(encode_event(event)) == event
    assert event["noise_level"] is None
    assert event["speech_flag"] is None


@pytest.mark.parametrize("field,value",[("n_person",-1),("n_person",True),("object_count",0),
    ("noise_level",float("nan")),("noise_level",1.1),("timestamp",float("inf")),
    ("bias_flag",.5),("source_id",""),("event_id",None),("source_kind","random")])
def test_invalid_event_rejected(field,value):
    event = new_event("PC_camera",1,2)
    event[field] = value
    with pytest.raises(ValueError):
        validate_event(event)


@pytest.mark.parametrize("persons,noise,audio,count,expected",[
    (3,.299,10,10,"ALERT"),(3,.3,10,10,"NORMAL"),(2.99,.1,10,10,"NORMAL"),
    (4,None,0,10,"UNAVAILABLE"),(4,.1,9,10,"UNAVAILABLE"),
    (None,.1,10,10,"UNAVAILABLE"),(4,float("nan"),10,10,"UNAVAILABLE")])
def test_alert_thresholds_and_missing(persons,noise,audio,count,expected):
    assert alert_state(dict(avg_person=persons,avg_noise=noise,audio_samples=audio,event_count=count)) == expected
