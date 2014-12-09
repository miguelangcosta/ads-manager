package ads.manager.common;

public class AdEvent {

    // The banner/campaign ID
    private int bannerId;
    // The ID of the promoted app
    private int appId;
    // The unique user ID
    private int userId;
    // The unix timestamp at event time
    private long timestamp;
    /*
       0 - view_banner – a banner was displayed
       1 - click_banner – a banner was clicked
       2 - open_app – a app was opened
    */
    private int eventType;


    /** Get the id of the user.
     * @return the id of the user
    */
    public int getUserId() {
        return userId;
    }

    /** Set the id of the user.
     * @param userId the id of the user
     */
    public void setUserId(int userId) {
        this.userId = userId;
    }

    /** Get the id of the banner.
     * @return the id of the banner.
     */
    public int getBannerId() {
        return bannerId;
    }

    /** Set the id of the banner.
     * @param bannerId the id of the banner.
     */
    public void setBannerId(int bannerId) {
        this.bannerId = bannerId;
    }

    /** Get the id of the app.
     * @return the id of the app.
     */
    public int getAppId() {
        return appId;
    }

    /** Set the id of the app.
     * @param appId the id of the app.
     */
    public void setAppId(int appId) {
        this.appId = appId;
    }

    /** Get the timestamp of the event.
     * @return the timestamp of the event.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /** Set the timestamp of the event.
     * @param timestamp the timestamp of the event.
     */
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /** Get the event type.
     * @return the event type.
     */
    public int getEventType() {
        return eventType;
    }

    /** Set the event type.
     * @param eventType the type of the event.
     */
    public void setEventType(int eventType) {
        this.eventType = eventType;
    }
}
