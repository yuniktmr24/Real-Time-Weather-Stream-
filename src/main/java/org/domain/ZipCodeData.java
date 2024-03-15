package org.domain;

public class ZipCodeData {
    private String zip;
    private String city;
    private String stateId;
    private String stateName;

    public ZipCodeData(String zip, String city, String stateId, String stateName) {
        this.zip = zip;
        this.city = city;
        this.stateId = stateId;
        this.stateName = stateName;
    }

    @Override
    public String toString() {
        return "ZipCodeData{" +
                "zip='" + zip + '\'' +
                ", city='" + city + '\'' +
                ", stateId='" + stateId + '\'' +
                ", stateName='" + stateName + '\'' +
                '}';
    }

    public String getZip() {
        return zip;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getStateId() {
        return stateId;
    }

    public void setStateId(String stateId) {
        this.stateId = stateId;
    }

    public String getStateName() {
        return stateName;
    }

    public void setStateName(String stateName) {
        this.stateName = stateName;
    }
}
