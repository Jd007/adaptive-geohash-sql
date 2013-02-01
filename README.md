#### Adaptive Geohash Nearby SQL Search

Using the geohash value to find places near a given point of interest in a SQL table.

The search radius is increased adaptively (based on how many results we find) until the desired number of rows have been returned (or until we run out of rows).

Requirements:
* python-geohash
* SQLAlchemy (Expression Language API)

Can be easily adapted into other database connection APIs. May not be 100% reliable so an alternative search method is still recommended for when the geohash search method fails.