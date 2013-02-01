import geohash
from math import radians, sin, cos, sqrt, atan2, pi
from sqlalchemy.sql import select, func

EQUATORIAL_R = 6367.445 # Equatorial radius of Earth in km (meridional radius, preferred)
# EQUATORIAL_R = 6372.797 # Equatorial radius of Earth in km (mean great circle radius, alternative)

def distance(lat1, long1, lat2, long2):
	# Returns the geographic distance between the coordinates (lat1, long1) and (lat2, long2) in km
	if lat1 == lat2 and long1 == long2:
		return 0
	dlong = radians(long1 - long2)
	dlat = radians(lat1 - lat2)
	a = sin(dlat / 2.0) * sin(dlat / 2.0) + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlong / 2.0) * sin(dlong / 2.0)
	b = 2.0 * atan2(sqrt(a), sqrt(1.0 - a))
	return EQUATORIAL_R * b * 1.0

class InvalidParamException(Exception):
	pass

def get_geohash_where_clause(coord_geoint, precision, geoint_col_name):
	'''
	Given an unsigned 64-bit int geohash, a precision, and the SQL table
	column name of the geohash field, returns a SQL WHERE clause that
	would select all rows within the distance bounded by the specified
	precision. If the precision is low enough such that no WHERE clauses
	can be generated, empty str is returned.

	coord_geoint: unsigned 64-bit int, geohash value of the coordinates
					around which to query for.
	precision: the precision to search within, the lower the value the
				larger the distance to search for. Must be between 0
				and 64.
	geoint_col_name: the name of the geohash int column in the SQL
					database table to search in.
	'''

	sub_clauses = []
	for bound_range in geohash.expand_uint64(coord_geoint, precision):
		if bound_range[0] and bound_range[1]:
			sub_clauses.append("(" + geoint_col_name + ">=" + str(bound_range[0]) + " AND " + geoint_col_name + "<" + str(bound_range[1]) + ")")
		elif bound_range[0]:
			sub_clauses.append("(" + geoint_col_name + ">=" + str(bound_range[0]) + ")")
		elif bound_range[1]:
			sub_clauses.append("(" + geoint_col_name + "<" + str(bound_range[1]) + ")")
	if len(sub_clauses) == 0:
		return ""
	else:
		return "(" + " OR ".join(sub_clauses) + ")"

def adaptive_geohash_nearby_search(latitude, longitude, lower_cut, upper_cut, select_cols, count_col, lat_col, lon_col,
									geoint_col, custom_where_clause, db_conn):
	'''
	Given the latitude, longitude of a point of interest, the lower and upper
	cut in the result pagination, the columns to select, the primary key
	column, the latitude and longitude columns, the geohash column, a
	custom SQL WHERE clause string to add, and a SQLAlchemy database
	connection, returns a list of selected SQLAlchemy rows as requested in
	the range of lower_cut and upper_cut, ordered by distance to the point
	provided, ascending. Returns None if the geohash search failed (should
	fallback to other search methods).

	latitude: latitude of the point to search around.
	longitude: longitude of the point to search around.
	lower_cut: the starting slice index of the results wanted (same rule as
				list slicing).
	upper_cut: the ending slice index of the results wanted (same rule as
				list slicing).
	select_cols: list of SQLAlchemy table column objects to select in the
				result.
	count_col: a SQLAlchemy table column object that is in the table used
				to count potential results, the primary key column is
				recommended, but any other singly unique column would work.
	lat_col: a SQLAlchemy table column object, the latitude column in the
				table.
	lon_col: a SQLAlchemy table column object, the longitude column in the
				table.
	geoint_col: a SQLAlchemy table column object, the geohash column in the
				table.
	custom_where_clause: any custom SQL WHERE conditions to add in addition
						to the nearby query. Must be sanitized, and lead
						with the logic connector (e.g. AND) to join with
						the geohash queries.
	db_conn: a SQLAlchemy database connection, used to perform the database
			query.
	'''

	if not (isinstance(latitude, (int, float)) or not isinstance(longitude, (int, float)) or not isinstance(lower_cut, int) or not isinstance(upper_cut, int)):
		raise InvalidParamException()
	if lat_col not in select_cols:
		select_cols.append(lat_col)
	if lon_col not in select_cols:
		select_cols.append(lon_col)
	max_result_cap = upper_cut
	# Geohash may give inaccurate results for smaller result numbers, so
	# make sure we at least get 20 rows, which can be discarded later
	max_result_cap = max(max_result_cap, 20)
	coord_geoint = geohash.encode_uint64(latitude, longitude)
	MAX_DB_HITS = 20 # A limit on the maximum number of repeated database queries before giving up
	precision = 50 # Starting precision
	previous_precision = 50
	previous_result_size = 0
	previous_where_clause = ''
	stop = False
	fall_back = False
	loop_count = 0
	while not stop and not fall_back:
		where_clause = get_geohash_where_clause(coord_geoint, precision, geoint_col.name)
		if len(where_clause) == 0:
			# Cannot find anything, fallback to other search methods
			stop = True
			fall_back = True
		else:
			if len(custom_where_clause) > 0:
				where_clause += custom_where_clause
			data_count = db_conn.execute(select(columns=[func.count(count_col)],
											whereclause=where_clause)).first()
			result_size = data_count[0]
			loop_count += 1
			if result_size < previous_result_size:
				# Precision getting too low, result size shrinking, stop and use the previous where clause
				where_clause = previous_where_clause
				stop = True
			else:
				previous_result_size = result_size
				previous_precision = precision
				previous_where_clause = where_clause
			if result_size >= max_result_cap:
				stop = True
			if result_size < max_result_cap and loop_count >= MAX_DB_HITS:
				# Hit the max database query limit, fall back to other search methods
				stop = True
				fall_back = True
			else:
				percent_returned = float(result_size) / float(max_result_cap)
				# Adaptively decrease the search precision based on the percent of rows
				# we have found, while making sure that the precision does not decrease
				# to too low
				if percent_returned == 0 and (1.0 / float(max_result_cap)) < 0.05:
					if precision <= 4:
						precision -= 1
					else:
						precision -= 8
						precision = max(4, precision)
				elif percent_returned == 0 and (1.0 / float(max_result_cap)) >= 0.05:
					if precision <= 4:
						precision -= 1
					else:
						precision -= 7
						precision = max(4, precision)
				elif percent_returned <= 0.05:
					if precision <= 4:
						precision -= 1
					else:
						precision -= 6
						precision = max(4, precision)
				elif percent_returned < 0.25:
					if precision <= 4:
						precision -= 1
					else:
						precision -= 4
						precision = max(4, precision)
				elif percent_returned < 0.5:
					if precision <= 3:
						precision -= 1
					else:
						precision -= 2
						precision = max(3, precision)
				else:
					precision -= 1
				precision = max(0, precision)
	if not fall_back:
		# Actually fetch the results based on the last WHERE clause used, which is successful
		data = db_conn.execute(select(columns=select_cols,
									whereclause=where_clause)).fetchall()
		sorted_list = []
		for row in data:
			sorted_list.append([row, distance(latitude, longitude, row[lat_col], row[lon_col])])
		# Sort the results by distance
		sorted_list = sorted(sorted_list, key=lambda sort_elem : sort_elem[1])
		return [i[0] for i in sorted_list][lower_cut:upper_cut]
	else:
		return None