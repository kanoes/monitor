import csv

def create_csv(response):
    with open("output/result.csv", "w", encoding="ANSI", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(response.tables[0].columns)
        for row in response.tables[0].rows:
            writer.writerow(row)