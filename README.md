# User_Geo_Location

Make sure to fill up the twitter_credentials.py file with your API keys. 

![alt text](https://developers.gigya.com/download/attachments/8570128/twitter_keys.jpg?version=1&modificationDate=1431476196000&api=v2)

Make sure that you have all requirements installed (cf readme.pdf)

You can run get_ULDS.py to insert a set of users in a database, and, once this is done, get_tweets.py to recover the timeline of the later set of users.

To run get_ULDS.py, you must redefine the location_terms characteristic of the city or town you target, as well as the set of seed users you know for sure are in the location (screen names) from which the model will run the first iteration.

Similarly, language, UTC offset and geocode name ought to be tailored to your target location.

