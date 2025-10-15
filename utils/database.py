# ONLY MOCK!

data = {
    "143348144": 112
}

def read_track_from_db(track_id):
    return data.get(track_id)

def write_track_into_db(track_id, bpm):
    data[track_id] = bpm

def show_db():
    print(data)