#CircleNetPage data generator

# require: ID, nickname, jobtitle, regioncode, favorite hobby
# potential optional fields:
# first name, last name, age, gender

from faker import Faker
import random
from datetime import datetime
import faker 


from util.config import HOBBIES, ActionType2
from util.config import SMALL_CONFIG, FULL_CONFIG
from util.config import JOB_TITLES

from util.config import FOLLOW_REASONS
from util.config import DESCRIPTION_PHRASES
from util.config import ActionType

seed = 42
random.seed(seed)
fake = Faker()
fake.seed_instance(seed)


#ID Generator - unique sequential number (integer) from 1 to 200,000
# indicating the owner of the page (there will be 200,000 lines)
def id_gen(index):
    return index + 1

#Action ID Generator - unique sequential number (integer) from 1 to 10,000,000
# indicating the ID of the action on the page (there will be 10,000,000 lines)
def action_id_gen(index):
    return index + 1

# NickName - characters of length between 10 and 20 (do not use commas
#inside this string)

def nickname_gen(fake):
    nickname = fake.user_name().replace(",", "")
    if len(nickname) < 10:
        return nickname_gen(fake)
    if len(nickname) > 20:
        nickname = nickname[:20]
    return nickname

# JobTitle - characters of length between 10 and 20 (do not use commas
# inside this string)
def jobtitle_gen(fake):
    job = fake.job()
    job = job.replace(",", "")

    if 10 <= len(job) <= 20:
        return job
    return random.choice(JOB_TITLES)

# RegionCode - integer between 1 and 50
def regioncode_gen():
    return random.randint(1,50)

# FavoriteHobby - sequence of characters between 5 and 30
# look at config.py for list of HOBBIES and select randomly from list

def hobby_gen():
    hobby = random.choice(HOBBIES)
    if 5 <= len(hobby) <= 30:
        return hobby
    else:
        return "hobbying"


# FavoriteHobby - sequence of characters between 5 and 30
# look at config.py for list of HOBBIES and select randomly from list

def action_type_gen():
    action_type = random.choice(ActionType2)
    return str(action_type[0]) + str(" - ") + str(action_type[1])

# Action time list - integer between 1 and 50
def action_time_gene():
    return random.randint(1,100000)

#DEFINE SMALL/FULL RUN - keep on small for testing

def generate_circlenet_set(mode="small", out_path=None):
    if out_path is None:
        out_path = f"CircleNetPage_{mode}_{seed}.csv"

    if mode == "small":
        n_pages = SMALL_CONFIG["num_pages"]
    else:
        n_pages = FULL_CONFIG["num_pages"]

    f = open(out_path, "w")

    for i in range(n_pages):
        page_ID = id_gen(i)
        nickname = nickname_gen(fake)
        jobtitle = jobtitle_gen(fake)
        regioncode = regioncode_gen()
        hobby = hobby_gen()

        # Can include header if desired for checking formatting:
        # headers = "page_ID,nickname,jobtitle,regioncode,hobby\n"
        # f.write(headers) if i == 0 else None
        line = str(page_ID) + "," + nickname + "," + jobtitle + "," + str(regioncode) + "," + hobby + "\n"
        f.write(line)

    f.close()




#FOLLOWS DATASET GENERATOR

def follow_id_gen(index):
    return index + 1

#select random id from CircleNetPage
def follow_id_1(num_pages):
    return random.randint(1, num_pages)

#add check to ensure same relationship doesn't already exist
#select random id again that isn't same as first id
def follow_id_2(follow_1, num_pages):
    follow_2 = random.randint(1, num_pages)
    if follow_2 == follow_1:
        return follow_id_2(follow_1, num_pages)
    return follow_2

def relation_date_gen():
    start_date = int(datetime(2020, 1, 1).timestamp())
    end_date = int(datetime(2025, 12, 31).timestamp())
    return random.randint(start_date, end_date)

def description_gen():
    for _ in range(10):
        reason = random.choice(FOLLOW_REASONS)
        template = random.choice(DESCRIPTION_PHRASES)
        desc = template.format(reason=reason).replace(",", "")
        if 20 <= len(desc) <= 50:
            return desc
    return "Followed for an unspecified reason"
        
def generate_follows_set(mode="small", out_path=None):
    if out_path is None:
        out_path = f"CircleNetFollows_{mode}_{seed}.csv"

    if mode == "small":
        n_pages = SMALL_CONFIG["num_pages"]
        n_follows = SMALL_CONFIG["num_follows"]
    else:
        n_pages = FULL_CONFIG["num_pages"]
        n_follows = FULL_CONFIG["num_follows"]

    f = open(out_path, "w", encoding="utf-8")

    for i in range(n_follows):
        follow_ID = follow_id_gen(i)
        follower_ID = follow_id_1(n_pages)
        followee_ID = follow_id_2(follower_ID, n_pages)
        date = relation_date_gen()
        description = description_gen()

        # Can include header if desired for checking formatting:
        # headers = "follow_ID,follower_ID,followee_ID,day,month,year,description\n"
        # f.write(headers) if i == 0 else None
        line = (str(follow_ID) + "," + 
                str(follower_ID) + "," + 
                str(followee_ID) + "," + 
                str(date) + "," +
                description + "\n")
        f.write(line)

    f.close()


#change to large for full size data set
generate_circlenet_set("small")

# use small for testing, full or anything else for full size
generate_follows_set("small")


def generate_activity_log_set(mode="small", out_path=None):
    if out_path is None:
        out_path = f"ActivityLog_{mode}_{seed}.csv"

    if mode == "small":
        n_pages = SMALL_CONFIG["num_pages"]
    else:
        n_pages = FULL_CONFIG["num_pages"]

    f = open(out_path, "w")

    for i in range(n_pages):
        bywho = id_gen(i)
        action_id = action_id_gen(i)
        action_type = action_type_gen()
        action_time = random.randint(1, 10000000)
        whatpage = random.randint(1, 1000)

        line = str(bywho) + "," + str(action_id) + "," + str(action_time) + "," + str(whatpage) + "," + str(action_type) + "\n"
        f.write(line)

    f.close()


generate_activity_log_set()