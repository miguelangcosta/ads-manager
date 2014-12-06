import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by miguelcosta on 12/6/14.
 */
public class Driver {

    /*banner_id: the banner/campaign ID
    app_id: the ID of the promoted game
    user_id: the unique user ID
    timestamp: the unix timestamp at event time
    event_type:
            view_banner – a banner was displayed
                          Can happen multiple times
            click_banner – a banner was clicked in the game (this opens the app store, displaying the game)
                          Can happen multiple times
            open_app – the promoted app was opened
                        Can happen multiple times
                        If there’s no campaign running, banner_id will be empty
                        Can be triggered multiple times
    */

    private static final Logger log = LoggerFactory.getLogger(Driver.class);
    private static Random RANDOM = new Random(System.nanoTime());
    private final ObjectMapper jsonMapper = new ObjectMapper();

    private final int BANNERS = 10;
    private final int APPS = 100;
    private final int USERS = 1000;
    private final int NR_EVENTS = 10000000;
    /*
    EventType:
      0 - view_banner
      1 - click_banner
      2 - open_app
    */

    private final int EVENT_TYPE = 3;

    //TO DO - RUN AS A SERVICE
    public static void main(String[] args) {

        Driver driver = new Driver();
        driver.createAdsEvents();
    }


    /**
     * Creates data to simulate an ads platform
     */
    private void createAdsEvents(){

        //TO DO RUN THIS WITH MULTIPLE THREADS
        for(int i = 0; i < NR_EVENTS; i++){

            Date date = new Date();

            long timestamp = date.getTime();
            int bannerId = RANDOM.nextInt(BANNERS);
            int userId = RANDOM.nextInt(USERS);
            int appId = RANDOM.nextInt(APPS);
            int eventType = RANDOM.nextInt(EVENT_TYPE);
            Map<String, String> adEvent =  new HashMap<String, String>();
            adEvent.put("BannerId", String.valueOf(bannerId));
            adEvent.put("UserId", String.valueOf(userId));
            adEvent.put("AppId", String.valueOf(appId));
            adEvent.put("EventType", String.valueOf(eventType));
            adEvent.put("Timestamp", String.valueOf(timestamp));

            try {
                String message = jsonMapper.writeValueAsString(adEvent);
                System.out.println(message);
            } catch (JsonProcessingException e) {
                log.error(e.getMessage());
                System.exit(-1);
            }
        }

    }
    /**
     * Returns a pseudo-random number between min and max, inclusive.
     * The difference between min and max can be at most
     * <code>Integer.MAX_VALUE - 1</code>.
     *
     * @param min Minimum value
     * @param max Maximum value.  Must be greater than min.
     * @return Integer between min and max, inclusive.
     * @see java.util.Random#nextInt(int)
     */
}
