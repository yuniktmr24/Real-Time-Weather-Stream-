package org.requests;

public class AQIResponse {
    private String zip;

    private String location;

    private String state;

    private int aqi;

    public AQIResponse(String zip, String location, String state, int aqi) {
        this.zip = zip;
        this.location = location;
        this.aqi = aqi;
        this.state = state;
    }

    public String getZip() {
        return zip;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public int getAqi() {
        return aqi;
    }

    public void setAqi(int aqi) {
        this.aqi = aqi;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }


    @Override
    public String toString() {
        return "AQIResponse{" +
                "zip='" + zip + '\'' +
                ", location='" + location + '\'' +
                ", state='" + state + '\'' +
                ", aqi=" + aqi +
                '}';
    }
}
