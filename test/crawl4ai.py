import asyncio
import os
from crawl4ai import AsyncWebCrawler
from openai import OpenAI

# --- Configuration ---
OPENAI_API_KEY=""
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
client = OpenAI(api_key=OPENAI_API_KEY)

# URL cible
URL = "https://www.rekrute.com/offres.html?st=d&keywordNew=1&jobLocation=RK&tagSearchKey=&keyword=data"

# Prompt pour GPT
PROMPT = """
Tu es un extracteur spécialisé en offres d'emploi.

À partir de cette page Rekrute.com, extrait toutes les offres disponibles sous forme de tableau JSON contenant :
- Titre de l'offre
- Nom de l'entreprise
- URL de l'offre
- Date de publication
- Type de contrat
- Région
- Secteur d'activité
- Niveau d'expérience requis
- Niveau d'études requis
- Compétences clés

Format de sortie : uniquement un tableau JSON valide, sans texte supplémentaire.
"""

async def scrape_and_extract_with_gpt():
    async with AsyncWebCrawler(verbose=True) as crawler:
        result = await crawler.arun(
            url=URL,
            js_code="""
            // Attendre que les offres soient chargées
            const waitForJobs = () => {
                if (document.querySelectorAll('.offer').length > 0) {
                    return 'Offres trouvées';
                }
                return false;
            };
            return waitForJobs();
            """
        )

        if not result.success:
            print("[❌] Échec lors du chargement de la page.")
            return None

        print("[✅] Page chargée avec succès.")

        # Appel à l'API OpenAI pour extraire les données
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "Vous êtes un assistant spécialisé dans l'extraction structurée d'informations."},
                {"role": "user", "content": f"{PROMPT}\n\nVoici le HTML de la page emploi :\n\n{result.cleaned_html}"}
            ]
        )

        extracted_data = completion.choices[0].message.content
        return extracted_data


async def main():
    print("[🔍] Démarrage du scraping et extraction IA...\n")
    extracted_content = await scrape_and_extract_with_gpt()

    if extracted_content:
        print("\n🧠 Données extraites par GPT :")
        print(extracted_content)

        # Sauvegarder dans un fichier JSON
        with open("offres_emploi_gpt.json", "w", encoding="utf-8") as f:
            f.write(extracted_content)

        print("\n💾 Données sauvegardées dans 'offres_emploi_gpt.json'")
    else:
        print("\n❌ Aucune donnée extraite.")


if __name__ == "__main__":
    asyncio.run(main())