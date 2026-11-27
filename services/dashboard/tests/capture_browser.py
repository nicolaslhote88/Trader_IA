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
        page=browser.new_page(viewport={"width":1536,"height":1080},device_scale_factor=1,color_scheme="dark")
        page.goto(args.url)
        for title,stem in PAGES:
            sidebar=page.locator('[data-testid="stSidebar"]')
            sidebar.get_by_role("button",name=title,exact=True).click(timeout=90000)
            expect(page.get_by_role("heading",name=title,exact=True).first).to_be_visible(timeout=60000)
            expect(page.get_by_role("button",name="Stop",exact=True)).not_to_be_visible(timeout=90000)
            evidence=page.get_by_text("Détails et preuves",exact=True).first
            expect(evidence).to_be_attached(timeout=60000)
            proof=evidence.locator("xpath=ancestor::details[1]")
            expect(proof).not_to_have_attribute("open","")
            evidence.click()
            expect(proof).to_have_attribute("open","")
            evidence.click()
            expect(proof).not_to_have_attribute("open","")
            page.locator('[data-testid="stMain"]').evaluate("(el)=>el.scrollTo(0,0)")
            sidebar.evaluate("(el)=>{for(const x of [el,...el.querySelectorAll('*')]) if(x.scrollTop)x.scrollTop=0}")
            plots=page.locator('[data-testid="stMain"] .js-plotly-plot:visible')
            expect(plots.first).to_be_visible(timeout=45000)
            page.wait_for_function("""()=>Array.from(document.querySelectorAll('.js-plotly-plot')).some(e=>e.clientWidth>0 && e.querySelector('svg.main-svg .cartesianlayer, svg.main-svg .pielayer'))""",timeout=45000)
            main=page.locator('[data-testid="stMain"]')
            height=min(4000,max(1080,main.evaluate("(el)=>el.scrollHeight")+80))
            page.set_viewport_size({"width":1536,"height":height})
            main.evaluate("(el)=>el.scrollTo(0,0)")
            page.screenshot(path=str(args.output/(stem+".png")),animations="disabled")
            page.set_viewport_size({"width":1536,"height":1080})
            print(stem,flush=True)
        browser.close()


if __name__=="__main__":
    main()
