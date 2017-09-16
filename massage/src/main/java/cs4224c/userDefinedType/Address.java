package cs4224c.userDefinedType;

public class Address {

    public String street_1;
    public String street_2;
    public String city;
    public String state;
    public String zip;

    public Address(String street_1, String street_2, String city, String state, String zip) {
        this.street_1 = street_1;
        this.street_2 = street_2;
        this.city = city;
        this.state = state;
        this.zip = zip;
    }

    @Override
    public String toString() {
        return String.format("{street_1: '%s', street_2: '%s', city: '%s', state: '%s', zip: '%s'}",
                street_1, street_2, city, street_2, zip);
    }
}
