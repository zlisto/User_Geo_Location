# User_Geo_Location

Make sure to fill up the twitter_credentials.py file with your API keys. 

![alt text](https://developers.gigya.com/download/attachments/8570128/twitter_keys.jpg?version=1&modificationDate=1431476196000&api=v2)

Make sure that you have all requirements installed (cf readme.pdf)

You can run get_ULDS.py to insert a set of users in a database, and, once this is done, get_tweets.py to recover the timeline of the later set of users.

To run get_ULDS.py, you must redefine the location_terms characteristic of the city or town you target.

![alt text](https://github.com/MITSocialNetworksThinkTank/User_Geo_Location/blob/master/miscellaneous/city_name_variations.png)


Similarly, language, UTC offset and geocode name ought to be tailored to your target location.

![alt text](https://github.com/MITSocialNetworksThinkTank/User_Geo_Location/blob/master/miscellaneous/location_information.png)

Finally, you'll need to manually search for a small sample of seed users you know are in the location for sure, and update the following:

![alt text](https://github.com/MITSocialNetworksThinkTank/User_Geo_Location/blob/master/miscellaneous/seed_users.png)

There exists various ways to get a user_id from a screen_name, one of which is going to https://tweeterid.com/:

![alt text]()
