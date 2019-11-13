package cn.edu.thu.dquality.back;

public class Record {
    private String deviceId;
    private String time;
    private Double value;

    public Record() {
    }

    public Record(String deviceId, String time, Double value) {
        this.deviceId = deviceId;
        this.time = time;
        this.value = value;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}
