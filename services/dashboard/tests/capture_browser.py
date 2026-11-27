"""Recette visuelle dans un profil de test local relié au dashboard par tunnel."""
import argparse
from pathlib import Path
from playwright.sync_api import sync_playwright, expect

PAGES = [("Cockpit","Cockpit"),("Mon compte","Compte"),("Coûts","Couts"),("Test de 6 mois","Test"),
         ("Enseignements","Enseignements"),("Chaîne de bout en bout","Sante")]


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("url")
    parser.add_argument("output",type=Path)
    parser.add_argument("--chrome",required=True)
    args=parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=True)
    with sync_playwright() as driver:
        browser=driver.chromium.launch(executable_path=args.chrome,headless=True)
        context=browser.new_context(viewport={"width":1536,"height":1080},device_scale_factor=1,color_scheme="dark")
        for title,stem in PAGES:
            page=context.new_page()
            page.goto(args.url)
            sidebar=page.locator('[data-testid="stSidebar"]')
            if title!="Cockpit":
                sidebar.get_by_role("button",name=title,exact=True).click(timeout=90000)
            expect(page.get_by_role("heading",name=title,exact=True).first).to_be_visible(timeout=60000)
            expect(page.get_by_role("button",name="Stop",exact=True)).not_to_be_visible(timeout=90000)
            # Attendre les composants frontaux de la nouvelle page avant de manipuler ses détails.
            plots=page.locator('[data-testid="stMain"] .js-plotly-plot:visible')
            expect(plots.first).to_be_visible(timeout=45000)
            proof=page.locator(f'.st-key-proof_{stem} details').first
            summary=proof.locator(":scope > summary")
            expect(summary).to_be_visible(timeout=45000)
            expect(proof).not_to_have_attribute("open","")
            summary.click()
            expect(proof).to_have_attribute("open","",timeout=10000)
            summary.click()
            expect(proof).not_to_have_attribute("open","",timeout=10000)
            page.wait_for_function("""()=>Array.from(document.querySelectorAll('.js-plotly-plot')).some(e=>e.clientWidth>0 && e.querySelector('svg.main-svg .cartesianlayer, svg.main-svg .pielayer'))""",timeout=45000)
            main=page.locator('[data-testid="stMain"]')
            height=min(4000,max(1080,main.evaluate("(el)=>el.scrollHeight")+80))
            page.set_viewport_size({"width":1536,"height":height})
            main.evaluate("(el)=>el.scrollTo(0,0)")
            page.screenshot(path=str(args.output/(stem+".png")),animations="disabled")
            page.close()
            print(stem,flush=True)
        browser.close()


if __name__=="__main__":
    main()
