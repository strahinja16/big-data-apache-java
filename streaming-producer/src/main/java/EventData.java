public class EventData {
    public static EventData CreateEventData(String data) throws Exception {
        String[] args = data.split(",");

        if (args.length < 13) {
            return null;
        }

        return new EventData(args);
    }

    private final String eventId;
    private final String type;
    private final String severity;
    private final String startTime;
    private final String endTime;
    private final String timeZone;
    private final String locationLat;
    private final String locationLng;
    private final String airportCode;
    private final String city;
    private final String country;
    private final String state;
    private final String zipCode;

    private EventData(String[] args) {
        this.eventId = args[0];
        this.type = args[1];
        this.severity = args[2];
        this.startTime = args[3];
        this.endTime = args[4];
        this.timeZone = args[5];
        this.locationLat = args[6];
        this.locationLng = args[7];
        this.airportCode = args[8];
        this.city = args[9];
        this.country = args[10];
        this.state = args[11];
        this.zipCode = args[12];
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", eventId,
            type, severity, startTime, endTime, timeZone, locationLat, locationLng,
            airportCode, city, country, state, zipCode
        );
    }

    public String getEventId() {
        return eventId;
    }

    public String getType() {
        return type;
    }

    public String getSeverity() {
        return severity;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public String getLocationLat() {
        return locationLat;
    }

    public String getLocationLng() {
        return locationLng;
    }

    public String getAirportCode() {
        return airportCode;
    }

    public String getCity() {
        return city;
    }

    public String getCountry() {
        return country;
    }

    public String getState() {
        return state;
    }

    public String getZipCode() {
        return zipCode;
    }
}
