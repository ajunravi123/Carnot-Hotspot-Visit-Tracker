import csv
import math
from time import gmtime, strftime, time

"""Defining Class for creating K-d tree node objects for each Hotspots"""
class Hotspot:
    def __init__(self, hotspot, axis, left=None, right=None):

        self.hotspot = hotspot
        self.axis = axis
        self.left = left
        self.right = right


# Storing the Hotspot master data in the k-d tree Data structure
def build_hotspot_tree(hotspots, depth=0):
    if not hotspots:
        return None

    axis = depth % 2
    hotspots = sorted(hotspots, key=lambda x: x['location'][axis])
    median = len(hotspots) // 2

    return Hotspot(
        hotspot=hotspots[median],
        axis=axis,
        left=build_hotspot_tree(hotspots[:median], depth + 1),
        right=build_hotspot_tree(hotspots[median+1:], depth + 1)
    )


# Defining Search method for finding the nearest Hotspot of a given location
def search_nearest_hotspot(hotspot_tree, target, depth=0, best=None):
    if hotspot_tree is None:
        return best

    if best is None or dist(target, hotspot_tree.hotspot["location"]) < dist(target, best["location"]):
        best = hotspot_tree.hotspot

    axis = depth % 2
    if target[axis] < hotspot_tree.hotspot["location"][axis]:
        next_hotspot_tree = hotspot_tree.left
        opposite_hotspot_tree = hotspot_tree.right
    else:
        next_hotspot_tree = hotspot_tree.right
        opposite_hotspot_tree = hotspot_tree.left

    best = search_nearest_hotspot(next_hotspot_tree, target, depth + 1, best)

    if opposite_hotspot_tree is not None and abs(target[axis] - hotspot_tree.hotspot["location"][axis]) < dist(target, best["location"]):
        best = search_nearest_hotspot(opposite_hotspot_tree, target, depth + 1, best)

    return best

def dist(a, b):
    return (a[0] - b[0])**2 + (a[1] - b[1])**2

def get_current_time():
   return strftime("%Y-%m-%d %H:%M:%S", gmtime())


#loading hotspot data
def load_hotspots(filename):
    hotspots = []
    with open(filename, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            hotspots.append(
                { 
                    "id" : row["id"],
                    "name" : row["name"],
                    "location" : (int(row["x"]),int(row["y"])),
                    "category" : row["category"]
                }
            )

    return hotspots

# Method to get the distance between two points
def distance_between_points(point_1, point_2):
    return math.sqrt((point_2[0] - point_1[0])**2 + (point_2[1] - point_1[1])**2)

#load user streams data
def load_user_streams(filename):
    user_streams = []
    with open(filename, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            user_streams.append(
                {
                    "stream_id" : row[""],
                    "location":  (int(row["x"]), int(row["y"])),
                    "time_stamp" : row["time_stamp"]
                }
            )

    return user_streams




if __name__ == "__main__":

    print(f"- Program Started at {get_current_time()}")
    hotspot_data = load_hotspots("data/hotspot_data.csv")
    user_streams = load_user_streams("data/raw_data.csv")

    hotspot_tree = build_hotspot_tree(hotspot_data)
    time_taken_to_process_streams = 0 #milliseconds
    print(f"- {len(hotspot_data)} Hotspots locations are initialized")

    # Writing the user visits info to a CSV file
    output_file = 'outputs/hotspot_visit_data.csv'
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        field = ["hotspot_id", "stream_id", "time_of_visit"]
        writer.writerow(field)
        total_visits = 0

        print(f"- Checking the Hotspot proximity for {len(user_streams)} user streams...")
        for user_data in user_streams:
            # Finding the nearest hotspot to the user
            start_time = time()
            nearest_hotspot = search_nearest_hotspot(hotspot_tree, user_data["location"])

            # Calculating the distance between the nearest hotspot and the user location
            distance = distance_between_points(nearest_hotspot["location"], user_data["location"])

            #Calculating the totla execution time for the streams 
            end_time = time()
            time_taken_to_process_streams += (end_time - start_time) * 1000

            # Checking whether the user location is within the Hotspot Radius
            if distance <= 5:
                total_visits += 1
                writer.writerow([nearest_hotspot["id"], user_data["stream_id"], user_data["time_stamp"]])

        print(f"- User visits found for {total_visits} Hotspots")

    
    print(f"- Average time taken to process a single stream: {(time_taken_to_process_streams / len(user_streams)):.2f} Milliseconds")

    if (total_visits > 0): 
        print(f"- Hotspot visit details are successfully saved to '{output_file}'")
    else:
        print("No visits on any of the Hotspots")
    
    print(f"- Program Ended at {get_current_time()}")
