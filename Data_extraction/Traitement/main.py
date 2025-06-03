import concurrent.futures
import os
import re
import subprocess
import sys

from data_extraction.Websites import setup_logger

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)


# Configuration du logging
logger = setup_logger("main.log")


def execute_script(file_path):
    try:
        python_exe = (
            sys.executable
        )  # Path to the Python executable in the virtual environment
        result = subprocess.run(
            [python_exe, file_path], check=True, capture_output=True, text=True
        )
        if result.stdout:
            logger.info(f"Output from {file_path}:\n{result.stdout}")
            # Check for "Nombre d'offres:" in the output
            match = re.search(r"Nombre d'offres:\s*(\d+)", result.stdout)
            if match:
                offres_script = int(match.group(1))
                logger.info(f"Offers extracted by {file_path}: {offres_script}")
                return offres_script
            else:
                logger.warning(f"No offers found in the output of {file_path}.")
        return 0
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing {file_path}: Code {e.returncode} - {e.stderr}")
        return 0
    except Exception as e:
        logger.exception(f"Unexpected error with {file_path}: {str(e)}")
        return 0


def run_data_extraction_scripts():
    total_offres = 0
    logger.info("Démarrage du processus d'extraction des données.")
    traitement_dir = os.path.dirname(__file__)
    parent_dir = os.path.dirname(traitement_dir)

    extraction_dir = os.path.join(parent_dir, "Websites")

    if not os.path.exists(extraction_dir):
        logger.error(f"Le dossier {extraction_dir} n'existe pas!")
        return total_offres

    py_files = [f for f in os.listdir(extraction_dir) if f.endswith(".py")]
    logger.info(f"Scripts trouvés: {py_files}")

    # Create a ThreadPoolExecutor to run scripts concurrently
    with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
        # Submit each script to the executor
        futures = {
            executor.submit(
                execute_script, os.path.join(extraction_dir, py_file)
            ): py_file
            for py_file in py_files
        }

        for future in concurrent.futures.as_completed(futures):
            py_file = futures[future]
            try:
                offres_script = future.result()
                total_offres += offres_script
                logger.info(f"Le script {py_file} est terminé")
                logger.info(f"Total offers so far: {total_offres}")
            except Exception as e:
                logger.error(f"Error with {py_file}: {str(e)}")

    logger.info("Fin du traitement de tous les scripts.")
    logger.info(f"Nombre total d'offres extraites: {total_offres}")
    return total_offres


if __name__ == "__main__":
    print("Début de l'extraction des données...")
    total = run_data_extraction_scripts()
    print("\nTous les scripts d'extraction ont été traités!")
    print(f"Nombre total d'offres extraites: {total}")