import json
import os

import spacy
from spacy.matcher import PhraseMatcher
from utils import make_buckets, read_all_from_bucket, save_to_minio

# load default skills data base
from skillNer.general_params import SKILL_DB

# import skill extractor
from skillNer.skill_extractor_class import SkillExtractor


def annotate_text(filename) -> list:
    """This functions uses spacy's NLP and  skillner's skill extractor and a custom skill database to annotate text.

    This text can displayed using skillextractor's describe and display methods.

    Parameters
    ----------
    filename:
      the name of the json file to extract text from and then annotate
    """
    # init params of skill extractor
    nlp = spacy.load("en_core_web_lg")
    file_path = os.path.join(os.getcwd(), filename)
    # init skill extractor
    skill_extractor = SkillExtractor(nlp, SKILL_DB, PhraseMatcher)

    job_offers = json.load(
        open(
            file_path,
            "r",
            encoding="utf-8",
        )
    )

    annotations = []

    for job_offer in job_offers:
        # Dans le cas de rekrute.com et emploi.ma on a les champs description et competences
        if "description" in job_offer:
            try:
                annotations.append(
                    skill_extractor.annotate(
                        job_offer["description"] + job_offer["competences"]
                    )
                )
            except Exception as e:
                print(f"Exception during the annotation phase for {filename} : {e}")
                continue
        # Dans le cas de marocannonces on a les champs fonction et domaine
        elif "fonction" in job_offer:
            try:
                annotations.append(
                    skill_extractor.annotate(
                        job_offer["fonction"] + job_offer["domaine"]
                    )
                )
            except Exception as e:
                print(f"Exception during the annotation phase for {filename} : {e}")
                continue
        else:
            continue
    return annotations


def add_skill(skill_id, skills: dict):
    # retrieving name and type of skill from skills database
    skill_name = SKILL_DB[skill_id]["skill_name"]
    skill_type = SKILL_DB[skill_id]["skill_type"]

    # checking for duplicate hard skill
    if skill_type == "Hard Skill" and skill_name not in skills["hard_skills"]:
        skills["hard_skills"].append(skill_name)
    # checking for duplicate soft skill
    if skill_type == "Soft Skill" and skill_name not in skills["soft_skills"]:
        skills["soft_skills"].append(skill_name)
    return skills


def extract_skills(filename) -> list:
    """Given the filename of a json file, this function will do NER on the skills present in the file's text.

    Parameters
    ---------
    filename:
        The name of the json file
    """
    #  Reading the initial file
    with open(filename, "r", encoding="utf-8") as f:
        original_data = json.load(f)

    # annotate the text
    annotations = annotate_text(filename=filename)
    merged_data = []

    # During this step we go through the annotations and match the skill id's from SKILL_DB with the skill names
    for i in range(len(annotations)):
        job_offer = annotations[i]
        skills = {"hard_skills": [], "soft_skills": []}
        # Checking the skill ids returned as a full match
        for full_match in job_offer["results"]["full_matches"]:
            skill_id = full_match["skill_id"]
            skills = add_skill(skill_id, skills)
        # Checking the skill ids returned after compatibility scoring
        for ngram_score in job_offer["results"]["ngram_scored"]:
            skill_id = ngram_score["skill_id"]
            skills = add_skill(skill_id, skills)

        # Merge NER skills into the original job offer
        original_entry = original_data[i]
        original_entry["skills"] = skills
        merged_data.append(original_entry)

    # Save the merged output to a file
    ner_filename = os.path.join("data", ("NER_" + os.path.basename(filename)))
    with open(ner_filename, "w", encoding="utf-8") as js_file:
        json.dump(merged_data, js_file, ensure_ascii=False, indent=4)

    save_to_minio(file_path=ner_filename, bucket_name="ner")
    os.remove(ner_filename)

    return merged_data


def skillner_extract_and_upload(json_folder="data"):
    json_path = os.path.join(os.getcwd(), json_folder)
    filenames = os.listdir(json_path)
    print(f"Preparing current files for skill extraction: {filenames}")
    try:
        for filename in filenames:
            # Checking if the file has the json extension
            ext = os.path.splitext(filename)[-1]
            if ext == ".json":
                print(f"Extracting skills from: {filename}")
                extract_skills(os.path.join(json_path, filename))
            else:
                continue
    except Exception as e:
        print(f"Couldn't extract skills from json: {e}")


def main():
    try:
        print("-------------Starting the Ner with skillner-------------")
        # getting a list of all directories to check existence of data folder
        files = os.listdir()
        if "data" not in files:  # makes the data folder if it doesnt exist
            print("Data folder not found, making one")
            try:
                os.mkdir("data")
                print("Success making the data folder")
            except Exception as e:
                print(f"Exception during creation og data folder: {e}")
        else:
            print("Data folder found, proceeding")
            pass
        # Finding or creating the necessary ner bucket
        try:
            print("Finding or creating the ner bucket")
            make_buckets(["ner"])
            print("Success finding the ner bucket")
        except Exception as e:
            print(f"Error during the bucket retrieval process: {e}")
        # Reading the data from the bucket
        try:
            print("Reading the json files present in the ner bucket")
            read_all_from_bucket(dest_dir="data")
            print("Success reading the json files")
        except Exception as e:
            print(f"Exception during json files reading :{e}")
        # Using skillner for ner to extract skills from the json files
        try:
            print("Extracting the skills from the json files")
            skillner_extract_and_upload(json_folder="data")
        except Exception as e:
            print(f"Exception during extraction of skills :{e}")
        print("-------------All steps were succesfull. End of program-------------")
    except Exception as e:
        print(f"Error during program: {e}")


if __name__ == "__main__":
    main()
