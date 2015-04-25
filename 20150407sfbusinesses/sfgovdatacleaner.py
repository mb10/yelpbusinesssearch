import csv

with open('20150422_Registered_Business_Locations_-_San_Francisco.csv', 'rU') as readfile, open('20150422_Registered_Business_Locations - Cleaned.csv', 'wb') as writefile, open('businesses_replaced.csv', 'wb') as replaced_biz_file:
		origin_file_contents = csv.DictReader(readfile)
		fieldnames = list(origin_file_contents.fieldnames)
		print fieldnames
		closedwriter = csv.DictWriter(writefile, fieldnames = fieldnames)


		closedwriter.writeheader()
		n = 0

		closed_addresses = []
		all_businesses = []

		for row in origin_file_contents:
			all_businesses.append(row)
			if row['Business_End_Date']:
				closed_addresses.append(row['Street_Address'])
				closedwriter.writerow(row)


"""
		for row in all_businesses:
			n += 1
			if row['Street_Address'] in closed_addresses:
				closedwriter.writerow(row)

		print n
		"""





