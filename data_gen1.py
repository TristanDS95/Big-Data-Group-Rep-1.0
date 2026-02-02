#CircleNetPage data generator

# require: ID, nickname, jobtitle, regioncode, favorite hobby
# potential optional fields:
# first name, last name, age, gender

from faker import Faker
import random

from util.config import HOBBIES
from util.config import SMALL_CONFIG, FULL_CONFIG
from util.config import JOB_TITLES

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


def action_type_gen():
    ActionType = random.choice(ActionType )
    if 5 <= len(ActionType ) <= 50:
        return ActionType
    else:
        return "ActionTypeing"

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


generate_circlenet_set()



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
        action_time = action_time_gene()

        line = str(bywho) + "," + str(action_id) + "," + str(action_time) + "\n"
        f.write(line)

    f.close()


generate_activity_log_set()