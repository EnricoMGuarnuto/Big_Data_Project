import streamlit as st
import plotly.graph_objects as go
from streamlit_plotly_events import plotly_events

# =====================
# 1. Dizionario categorie e sottocategorie
# =====================
categories = {
    "BEVERAGES": {
        "Water": 53,
        "Fruit-based Soft Drinks": 59,
        "Cola Soft Drinks": 37,
        "Iced Tea": 31,
        "Energy Drinks": 42,
        "Flavored Water": 10,
        "Juices": 81,
        "Syrups": 12,
    },
    "SPIRITS AND APERITIFS": {
        "Non-alcoholic Aperitifs": 8,
        "Alcoholic Aperitifs": 31,
        "Liqueurs": 32,
        "Grappa": 30,
        "Cognac and Brandy": 8,
        "Whisky": 9,
        "Amaro": 21,
        "Gin": 12,
        "Vodka": 10,
        "Rum": 11,
        "Ethyl Alcohol": 4,
    },
    "BEERS": {
        "Standard": 62,
        "Premium": 41,
        "Imported": 59,
        "Non-alcoholic": 4,
        "Flavored": 3,
    },
    "WINES": {
        "Brick Wines": 19,
        "White Wines": 117,
        "Sparkling White Wines": 38,
        "Red Wines": 156,
        "Sparkling Red Wines": 73,
        "Rosé Wines": 17,
        "Sparkling Rosé Wines": 12,
        "Sparkling Wines": 22,
        "Dessert Wines": 5,
    },
    "LONG-LIFE DAIRY": {
        "Long-life Milk": 59,
        "Plant-based Drinks": 40,
        "Cream and Béchamel": 25,
        "Condensed Milk": 1,
    },
    "HOUSEHOLD GOODS": {
        "Paper Towels": 14,
        "Tissues": 21,
        "Napkins": 20,
        "Film-aluminum-parchment paper": 21,
        "Baking Trays": 16,
        "Freezer Bags": 13,
        "Toothpicks and similar": 10,
        "Paper tableware": 11,
    },
    "BATH AND PERSONAL CARE": {
        "Wipes": 11,
        "Sanitary pads and liners": 55,
        "Adult diapers": 30,
        "Cotton": 8,
        "Cotton swabs": 4,
        "Toilet paper": 12,
        "Bar soaps": 13,
        "Liquid soaps": 32,
        "Intimate soaps": 19,
        "Toothpastes": 57,
        "Mouthwashes": 14,
        "Toothbrushes and accessories": 51,
        "Shampoos": 81,
        "Conditioners and after-shampoos": 42,
        "Hair dyes": 34,
        "Hairsprays and gels": 26,
        "Deodorants and talc": 85,
        "Bath and shower gels": 92,
        "Body treatments": 35,
        "Face creams and masks": 23,
        "Cleansing and makeup remover waters": 29,
        "Acetone": 2,
        "Pre-shave": 17,
        "Razors and men's accessories": 39,
        "Razors and women's accessories": 33,
        "Aftershave": 15,
        "Band-aids and pharmaceuticals": 14,
        "Trash bags": 18,
    },
    "LAUNDRY PRODUCTS": {
        "Hand wash": 7,
        "Liquid detergent": 42,
        "Powder and sheet detergent": 20,
        "Descalers": 11,
        "Fabric softeners": 36,
        "Laundry scents": 19,
        "Bleaches": 28,
        "Stain removers": 19,
    },
    "HOUSE CLEANING PRODUCTS": {
        "Bleach": 15,
        "Dish detergents": 38,
        "Dishwasher detergents": 43,
        "Dishwasher salt": 2,
        "Ironing products": 4,
        "Bathroom cleaners": 26,
        "Drain cleaners": 8,
        "Air fresheners": 21,
        "Anti-limescale": 7,
        "Detergents and disinfectants": 31,
        "Glass and furniture cleaners": 28,
        "Insecticides": 34,
        "Gloves": 25,
        "Sponges and cloths": 41,
    },
    "BABY PRODUCTS": {
        "Diapers": 42,
        "Cookies": 7,
        "Pasta and creams": 30,
        "Baby formula": 23,
        "Homogenized foods": 81,
        "Care and hygiene": 29,
    },
    "FRUITS AND VEGETABLES": {
        "Apples": 7, "Pears": 5, "Cherries": 3, "Strawberries": 2, "Apricots": 3,
        "Plums": 3, "Yellow Peaches": 4, "Grapes": 3, "Bananas": 3, "Kiwi": 2,
        "Pineapples": 2, "Avocados": 2, "Watermelons": 1, "Melons": 2, "Berries": 6,
        "Potatoes": 5, "Onions": 4, "Spring onions": 3, "Carrots": 4, "Radishes": 2,
        "Green beans": 2, "Tomatoes": 10, "Cucumbers": 1, "Peppers": 3,
        "Eggplants": 1, "Zucchini": 2, "Fennels": 2, "Celery": 2, "Cabbages": 4,
        "Broccoli": 2, "Pumpkins": 2, "Beets and chicory": 2, "Lettuce": 3,
        "Radicchio": 1, "Oranges": 3, "Mandarins": 3, "Grapefruits": 1,
        "Ready-to-eat raw vegetables": 37,
        "Ready-to-eat cooked vegetables": 10,
        "Soups and side dishes": 39,
        "Garlic and chilies": 4,
        "Leaf herbs": 14,
    },
    "LONG-LIFE FRUIT": {
        "Canned fruit": 11,
        "Fruit desserts": 16,
        "Dried fruit": 21,
        "Nuts": 32,
        "Other snacks": 43,
        "Dried legumes": 20,
        "Seeds": 7,
    },
    "SWEETS AND BREAKFAST": {
        "Snack cakes": 97, "Ready-made cakes": 19, "Cookies": 114, "Industrial pastries": 63,
        "Snacks and bars": 71, "Sweeteners": 12, "Candies": 58, "Flavors and decorations": 31,
        "Cocoa powder": 5, "Puddings": 13, "Spreads": 25, "Chocolate bars": 32,
        "Chocolates": 27, "Chocolate eggs": 10, "Cereals": 38, "Rusks": 36,
        "Milk modifiers and chocolate preparations": 12, "Jams": 52, "Honey": 11,
    },
    "FLOURS AND SUGAR": {
        "Sugar": 13, "Flour": 44, "Corn flour and polenta": 18,
        "Yeast": 21, "Salt": 14,
    },
    "PASTA AND RICE": {
        "Rice": 43, "Spelt and whole wheat pasta": 23, "Cereals and others": 18,
        "Pasta": 98, "Egg pasta": 47, "Soup and risotto mixes": 45,
        "Stock cubes and broths": 31,
    },
    "GLUTEN FREE": {
        "Pasta": 21, "Baked goods": 4, "Cookies": 18, "Rice cakes": 4,
        "Savory snacks": 13, "Sweet snacks": 23,
    },
    "BREAD AND BREADSTICKS": {
        "Breadsticks": 29, "Flatbreads": 7, "Rice cakes": 29, "Packaged bread": 53,
        "Long-life flatbreads": 13, "Breadcrumbs": 3, "Pizza and bread mixes": 8,
        "Crackers": 25, "Taralli": 15,
    },
    "HOT BEVERAGES": {
        "Ground coffee": 28, "Decaf ground coffee": 6, "Coffee pods and capsules": 46,
        "Coffee beans": 10, "Instant coffee": 21, "Barley coffee": 10,
        "Tea bags": 32, "Decaf tea bags": 8, "Instant chamomile": 6,
        "Chamomile tea bags": 8, "Other infusions": 34,
    },
    "SAUCES": {
        "Mayonnaise": 21, "Ketchup": 13, "Mustard": 8,
        "Savory spreads": 14, "Other sauces": 30,
    },
    "PASTA SAUCES": {
        "Ready-made sauces": 60, "Tomato puree": 30, "Tomato pulp": 21,
    },
    "SPICES": {
        "Spices": 19, "Herbs": 21, "Flavorings": 16,
    },
    "VINEGAR AND OIL": {
        "White vinegar": 10, "Balsamic vinegar": 9, "Vinegar-condiments": 12,
        "Olive oil": 46, "Corn oil": 5, "Peanut oil": 4, "Soybean oil": 2,
        "Sunflower oil": 6, "Frying oil": 4,
    },
    "SAVORY SNACKS": {
        "Chips and similar": 64, "Popcorn": 8, "Toasted snacks": 18,
    },
    "PET PRODUCTS": {
        "Wet cat food": 57, "Dry cat food": 32, "Cat snacks": 15,
        "Wet dog food": 41, "Dry dog food": 30, "Dog snacks": 25,
        "Dog hygiene and care": 7, "Cat litter": 10,
    },
    "CANNED GOODS": {
        "Peeled tomatoes": 7, "Peas": 9, "Beans": 16, "Corn": 9,
        "Other legumes and vegetables": 23, "Vegetables in oil": 41,
        "Vegetables in vinegar": 12, "Sweet and sour vegetables": 8,
        "Olives": 32, "Condiriso": 11, "Capers": 8, "Dried mushrooms": 5,
        "Canned meat": 12, "Tuna in oil": 51, "Tuna in water": 14,
        "Tuna with other condiments": 20, "Mackerel and sardines": 22,
        "Anchovies and similar": 20, "Other fish products": 10,
    },
    "REFRIGERATED": {
        "Yogurt": 60, "Fresh Milk": 15, "Eggs": 10, "Fresh Pasta": 45,
        "Butter and Margarine": 20, "Cheese and Cured Meats": 120,
        "Pies and Pizza dough": 25, "Fresh Juices": 15,
    },
    "FROZEN": {
        "Frozen Vegetables": 40, "Frozen Fish": 25, "Frozen Potatoes": 10,
        "Ice Creams": 50, "Pizzas": 30, "Frozen Ready Meals": 35,
    },
    "MEAT": {
        "Beef": 40, "Pork": 30, "Chicken": 50, "Sausages and Burgers": 25,
        "Cold Cuts": 70,
    },
    "FISH": {
        "Fresh Fish": 30, "Shellfish": 15, "Smoked Salmon": 10,
    },
}


# =====================
# 2. Stato (mock: in seguito prenderemo da Postgres)
# =====================
def get_status():
    status = {}
    for cat, subs in categories.items():
        # simuliamo: se almeno una sottocategoria ha <20 prodotti → rosso
        status[cat] = "red" if any(v < 20 for v in subs.values()) else "green"
    return status

# =====================
# 3. Funzione per disegnare rettangoli dinamici
# =====================
def draw_rectangles(labels, colors, level_name):
    fig = go.Figure()
    for i, label in enumerate(labels):
        fig.add_shape(
            type="rect",
            x0=i * 2, y0=0, x1=i * 2 + 1.5, y1=2,
            fillcolor=colors[label], opacity=0.6, line=dict(color="black"),
        )
        fig.add_annotation(
            x=i * 2 + 0.75, y=1,
            text=label, showarrow=False, font=dict(size=10, color="black"),
        )
    fig.update_layout(width=1000, height=300, clickmode="event+select",
                      title=f"Seleziona una {level_name}")
    return fig

# =====================
# 4. Navigazione multilivello
# =====================
if "level" not in st.session_state:
    st.session_state.level = "aisles"   # aisles → subcategories → shelves
if "selected_aisle" not in st.session_state:
    st.session_state.selected_aisle = None
if "selected_subcat" not in st.session_state:
    st.session_state.selected_subcat = None

st.title("🛒 Supermarket Interactive Map")

# ---- Livello 1: Corsie ----
if st.session_state.level == "aisles":
    aisle_status = get_status()
    fig = draw_rectangles(list(categories.keys()), aisle_status, "corsia")
    selected = plotly_events(fig)
    st.plotly_chart(fig)

    if selected:
        aisle_clicked = list(categories.keys())[selected[0]["pointIndex"]]
        st.session_state.selected_aisle = aisle_clicked
        st.session_state.level = "subcategories"
        st.experimental_rerun()

# ---- Livello 2: Sottocategorie ----
elif st.session_state.level == "subcategories":
    aisle = st.session_state.selected_aisle
    st.subheader(f"Corsia: {aisle}")
    subs = categories[aisle]
    sub_status = {s: ("red" if v < 20 else "green") for s, v in subs.items()}
    fig = draw_rectangles(list(subs.keys()), sub_status, "sottocategoria")
    selected = plotly_events(fig)
    st.plotly_chart(fig)

    if st.button("⬅️ Torna alle corsie"):
        st.session_state.level = "aisles"
        st.experimental_rerun()

    if selected:
        subcat_clicked = list(subs.keys())[selected[0]["pointIndex"]]
        st.session_state.selected_subcat = subcat_clicked
        st.session_state.level = "shelves"
        st.experimental_rerun()

# ---- Livello 3: Shelf ----
elif st.session_state.level == "shelves":
    aisle = st.session_state.selected_aisle
    subcat = st.session_state.selected_subcat
    st.subheader(f"Corsia: {aisle} → Sottocategoria: {subcat}")

    num_shelves = categories[aisle][subcat]
    shelves = [f"Shelf-{i+1}" for i in range(num_shelves)]
    shelf_status = {s: ("red" if i % 7 == 0 else "green") for i, s in enumerate(shelves)}

    fig = draw_rectangles(shelves[:15], shelf_status, "shelf")  # mostro max 15 alla volta
    st.plotly_chart(fig)

    if st.button("⬅️ Torna alle sottocategorie"):
        st.session_state.level = "subcategories"
        st.experimental_rerun()
