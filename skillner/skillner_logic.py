import json
import os

import spacy
from spacy.matcher import PhraseMatcher

from database import save_to_minio

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


def extract_skills(filename) -> list:
    """Given the filename of a json file, this function will do NER on the skills present in the file's text.

    Parameters
    ---------
    filename:
        The name of the json file
    """
    # First we annotate the text
    annotations = annotate_text(filename=filename)
    ner_data = []
    # During this step we go through the annotations and match the skill id's from SKILL_DB with the skill names
    for i in range(len(annotations)):
        job_offer = annotations[i]
        skills = {}

        for full_match in job_offer["results"]["full_matches"]:
            key = full_match["skill_id"]
            skills[SKILL_DB[key]["skill_name"]] = SKILL_DB[key]["skill_type"]

        for ngram_score in job_offer["results"]["ngram_scored"]:
            key = ngram_score["skill_id"]
            skills[SKILL_DB[key]["skill_name"]] = SKILL_DB[key]["skill_type"]
        ner_data.append(skills)

    ner_filename = "/tmp/" + "NER_" + filename
    with open(ner_filename, "w", encoding="utf-8") as js_file:
        json.dump(ner_data, js_file, ensure_ascii=False, indent=4)
    save_to_minio(file_path=ner_filename, bucket_name="web_scraping")
    return ner_data


def skillner_extract_and_upload():
    json_path = os.path.join(os.getcwd())
    filenames = os.listdir(json_path)
    print(f"Preparing current files for skill extraction: {filenames}")
    try:
        for filename in filenames:
            # Checking if the file has the json extension
            ext = os.path.splitext(filename)[-1]
            if ext == ".json":
                print(f"Extracting skills from: {filename}")
                extract_skills(filename=filename)
            else:
                continue
    except Exception as e:
        print(f"Couldn't extract skills from json: {e}")
