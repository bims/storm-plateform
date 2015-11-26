package otherClass;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import javax.annotation.Generated;

@Generated("org.jsonschema2pojo")
public class Tags {

    @SerializedName("addr:city")
    @Expose
    private String addrCity;
    @SerializedName("addr:housenumber")
    @Expose
    private String addrHousenumber;
    @SerializedName("addr:postcode")
    @Expose
    private String addrPostcode;
    @SerializedName("addr:street")
    @Expose
    private String addrStreet;
    @SerializedName("amenity")
    @Expose
    private String amenity;
    @SerializedName("name")
    @Expose
    private String name;
    @SerializedName("phone")
    @Expose
    private String phone;

    public String getAddrCity() {
        return addrCity;
    }

    public void setAddrCity(String addrCity) {
        this.addrCity = addrCity;
    }

    public String getAddrHousenumber() {
        return addrHousenumber;
    }

    public void setAddrHousenumber(String addrHousenumber) {
        this.addrHousenumber = addrHousenumber;
    }

    public String getAddrPostcode() {
        return addrPostcode;
    }

    public void setAddrPostcode(String addrPostcode) {
        this.addrPostcode = addrPostcode;
    }

    public String getAddrStreet() {
        return addrStreet;
    }

    public void setAddrStreet(String addrStreet) {
        this.addrStreet = addrStreet;
    }

    public String getAmenity() {
        return amenity;
    }

    public void setAmenity(String amenity) {
        this.amenity = amenity;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

}