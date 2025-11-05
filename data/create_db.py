import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Set seed for reproducibility
np.random.seed(42)

# Define product hierarchy with categories, subcategories, and their attributes
product_hierarchy = {
    'BEVERAGES': {
        'aisle': 1,
        'perishable': False,
        'subcategories': {
            'Water': {'count': 53, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (0.5, 2.5), 'weight_options': [500, 750, 1000, 1500, 2000]},
            'Fruit-based Soft Drinks': {'count': 59, 'store_max_stock_range': (15, 50), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [330, 500, 1000, 1500]},
            'Cola Soft Drinks': {'count': 37, 'store_max_stock_range': (15, 50), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.2, 3.5), 'weight_options': [330, 500, 1000, 1500]},
            'Iced Tea': {'count': 31, 'store_max_stock_range': (15, 45), 'warehouse_max_stock_range': (150, 350), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 2.8), 'weight_options': [330, 500, 1000, 1500]},
            'Energy Drinks': {'count': 42, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 250), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [250, 330, 500]},
            'Flavored Water': {'count': 10, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 2.5), 'weight_options': [500, 1000, 1500]},
            'Juices': {'count': 81, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.5), 'weight_options': [200, 500, 1000, 1500]},
            'Syrups': {'count': 12, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (100, 200), 'current_stock_percent': (0.7, 1.0), 'price_range': (3.0, 6.0), 'weight_options': [500, 750]},
        }
    },
    'SPIRITS AND APERITIFS': {
        'aisle': 2,
        'perishable': False,
        'subcategories': {
            'Non-alcoholic Aperitifs': {'count': 8, 'store_max_stock_range': (5, 15), 'warehouse_max_stock_range': (50, 150), 'current_stock_percent': (0.7, 1.0), 'price_range': (4.0, 8.0), 'weight_options': [750, 1000]},
            'Alcoholic Aperitifs': {'count': 31, 'store_max_stock_range': (5, 10), 'warehouse_max_stock_range': (50, 100), 'current_stock_percent': (0.7, 1.0), 'price_range': (7.0, 20.0), 'weight_options': [750, 1000]},
            'Liqueurs': {'count': 32, 'store_max_stock_range': (5, 10), 'warehouse_max_stock_range': (50, 100), 'current_stock_percent': (0.7, 1.0), 'price_range': (10.0, 30.0), 'weight_options': [500, 700, 1000]},
            'Grappa': {'count': 30, 'store_max_stock_range': (5, 8), 'warehouse_max_stock_range': (50, 80), 'current_stock_percent': (0.7, 1.0), 'price_range': (15.0, 50.0), 'weight_options': [500, 700]},
            'Cognac and Brandy': {'count': 8, 'store_max_stock_range': (4, 7), 'warehouse_max_stock_range': (40, 70), 'current_stock_percent': (0.7, 1.0), 'price_range': (25.0, 80.0), 'weight_options': [700, 1000]},
            'Whisky': {'count': 9, 'store_max_stock_range': (4, 7), 'warehouse_max_stock_range': (40, 70), 'current_stock_percent': (0.7, 1.0), 'price_range': (20.0, 60.0), 'weight_options': [700, 1000]},
            'Amaro': {'count': 21, 'store_max_stock_range': (5, 10), 'warehouse_max_stock_range': (50, 100), 'current_stock_percent': (0.7, 1.0), 'price_range': (12.0, 25.0), 'weight_options': [700, 1000]},
            'Gin': {'count': 12, 'store_max_stock_range': (4, 8), 'warehouse_max_stock_range': (40, 80), 'current_stock_percent': (0.7, 1.0), 'price_range': (15.0, 40.0), 'weight_options': [700, 1000]},
            'Vodka': {'count': 10, 'store_max_stock_range': (4, 8), 'warehouse_max_stock_range': (40, 80), 'current_stock_percent': (0.7, 1.0), 'price_range': (12.0, 30.0), 'weight_options': [700, 1000]},
            'Rum': {'count': 11, 'store_max_stock_range': (4, 8), 'warehouse_max_stock_range': (40, 80), 'current_stock_percent': (0.7, 1.0), 'price_range': (15.0, 45.0), 'weight_options': [700, 1000]},
            'Ethyl Alcohol': {'count': 4, 'store_max_stock_range': (10, 20), 'warehouse_max_stock_range': (100, 200), 'current_stock_percent': (0.7, 1.0), 'price_range': (5.0, 10.0), 'weight_options': [500, 1000]},
        }
    },
    'BEERS': {
        'aisle': 3,
        'perishable': False,
        'subcategories': {
            'Standard': {'count': 62, 'store_max_stock_range': (24, 72), 'warehouse_max_stock_range': (240, 720), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [330, 500, 660]},
            'Premium': {'count': 41, 'store_max_stock_range': (12, 36), 'warehouse_max_stock_range': (120, 360), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.5, 5.0), 'weight_options': [330, 500, 750]},
            'Imported': {'count': 59, 'store_max_stock_range': (12, 36), 'warehouse_max_stock_range': (120, 360), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 6.0), 'weight_options': [330, 500, 750]},
            'Non-alcoholic': {'count': 4, 'store_max_stock_range': (24, 48), 'warehouse_max_stock_range': (240, 480), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 3.0), 'weight_options': [330, 500]},
            'Flavored': {'count': 3, 'store_max_stock_range': (18, 30), 'warehouse_max_stock_range': (180, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 4.0), 'weight_options': [330, 500]},
        }
    },
    'WINES': {
        'aisle': 4,
        'perishable': False,
        'subcategories': {
            'Brick Wines': {'count': 19, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (100, 250), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 5.0), 'weight_options': [750, 1000, 1500]},
            'White Wines': {'count': 117, 'store_max_stock_range': (6, 12), 'warehouse_max_stock_range': (60, 120), 'current_stock_percent': (0.7, 1.0), 'price_range': (4.0, 30.0), 'weight_options': [750, 1500]},
            'Sparkling White Wines': {'count': 38, 'store_max_stock_range': (6, 10), 'warehouse_max_stock_range': (60, 100), 'current_stock_percent': (0.7, 1.0), 'price_range': (5.0, 25.0), 'weight_options': [750, 1500]},
            'Red Wines': {'count': 156, 'store_max_stock_range': (6, 12), 'warehouse_max_stock_range': (60, 120), 'current_stock_percent': (0.7, 1.0), 'price_range': (5.0, 50.0), 'weight_options': [750, 1500]},
            'Sparkling Red Wines': {'count': 73, 'store_max_stock_range': (6, 10), 'warehouse_max_stock_range': (60, 100), 'current_stock_percent': (0.7, 1.0), 'price_range': (5.0, 20.0), 'weight_options': [750, 1500]},
            'Rosé Wines': {'count': 17, 'store_max_stock_range': (6, 12), 'warehouse_max_stock_range': (60, 120), 'current_stock_percent': (0.7, 1.0), 'price_range': (4.0, 20.0), 'weight_options': [750, 1500]},
            'Sparkling Rosé Wines': {'count': 12, 'store_max_stock_range': (6, 10), 'warehouse_max_stock_range': (60, 100), 'current_stock_percent': (0.7, 1.0), 'price_range': (5.0, 20.0), 'weight_options': [750, 1500]},
            'Sparkling Wines': {'count': 22, 'store_max_stock_range': (6, 10), 'warehouse_max_stock_range': (60, 100), 'current_stock_percent': (0.7, 1.0), 'price_range': (8.0, 50.0), 'weight_options': [750, 1500]},
            'Dessert Wines': {'count': 5, 'store_max_stock_range': (4, 8), 'warehouse_max_stock_range': (40, 80), 'current_stock_percent': (0.7, 1.0), 'price_range': (10.0, 60.0), 'weight_options': [500, 750]},
        }
    },
    'LONG-LIFE DAIRY': {
        'aisle': 5,
        'perishable': False,
        'subcategories': {
            'Long-life Milk': {'count': 59, 'store_max_stock_range': (30, 100), 'warehouse_max_stock_range': (300, 1000), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 2.5), 'weight_options': [500, 1000]},
            'Plant-based Drinks': {'count': 40, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [500, 1000]},
            'Cream and Béchamel': {'count': 25, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.5), 'weight_options': [150, 250, 500]},
            'Condensed Milk': {'count': 1, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 3.0), 'weight_options': [170, 397]},
        }
    },
    'HOUSEHOLD GOODS': {
        'aisle': 6,
        'perishable': False,
        'subcategories': {
            'Paper Towels': {'count': 14, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 8.0), 'weight_range': (250, 1000)},
            'Tissues': {'count': 21, 'store_max_stock_range': (30, 80), 'warehouse_max_stock_range': (300, 800), 'current_stock_percent': (0.7, 1.0), 'price_range': (0.8, 3.0), 'weight_range': (50, 250)},
            'Napkins': {'count': 20, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 4.0), 'weight_range': (100, 500)},
            'Film-aluminum-parchment paper': {'count': 21, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 5.0), 'weight_range': (100, 500)},
            'Baking Trays': {'count': 16, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 6.0), 'weight_range': (100, 500)},
            'Freezer Bags': {'count': 13, 'store_max_stock_range': (15, 50), 'warehouse_max_stock_range': (150, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_range': (50, 300)},
            'Toothpicks and similar': {'count': 10, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (0.5, 2.0), 'weight_range': (20, 100)},
            'Paper tableware': {'count': 11, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 5.0), 'weight_range': (100, 500)},
        }
    },
    'BATH AND PERSONAL CARE': {
        'aisle': 7,
        'perishable': False,
        'subcategories': {
            'Wipes': {'count': 11, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 4.0), 'weight_range': (100, 500)},
            'Sanitary pads and liners': {'count': 55, 'store_max_stock_range': (15, 45), 'warehouse_max_stock_range': (150, 450), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 6.0), 'weight_range': (100, 400)},
            'Adult diapers': {'count': 30, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (8.0, 25.0), 'weight_range': (500, 2000)},
            'Cotton': {'count': 8, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_range': (50, 200)},
            'Cotton swabs': {'count': 4, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 2.5), 'weight_range': (50, 150)},
            'Toilet paper': {'count': 12, 'store_max_stock_range': (10, 40), 'warehouse_max_stock_range': (100, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.5, 10.0), 'weight_range': (500, 2000)},
            'Bar soaps': {'count': 13, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (0.8, 2.0), 'weight_range': (50, 200)},
            'Liquid soaps': {'count': 32, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [250, 500, 1000]},
            'Intimate soaps': {'count': 19, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_options': [250, 500]},
            'Toothpastes': {'count': 57, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_range': (50, 200)},
            'Mouthwashes': {'count': 14, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.5, 6.0), 'weight_options': [250, 500, 750]},
            'Toothbrushes and accessories': {'count': 51, 'store_max_stock_range': (15, 30), 'warehouse_max_stock_range': (150, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 5.0), 'weight_range': (50, 200)},
            'Shampoos': {'count': 81, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 8.0), 'weight_options': [250, 500, 750]},
            'Conditioners and after-shampoos': {'count': 42, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.5, 8.5), 'weight_options': [150, 250, 500]},
            'Hair dyes': {'count': 34, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (100, 250), 'current_stock_percent': (0.7, 1.0), 'price_range': (5.0, 15.0), 'weight_range': (100, 300)},
            'Hairsprays and gels': {'count': 26, 'store_max_stock_range': (15, 30), 'warehouse_max_stock_range': (150, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (3.0, 7.0), 'weight_range': (100, 500)},
            'Deodorants and talc': {'count': 85, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 6.0), 'weight_range': (50, 250)},
            'Bath and shower gels': {'count': 92, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 7.0), 'weight_options': [250, 500, 1000]},
            'Body treatments': {'count': 35, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (4.0, 12.0), 'weight_range': (100, 500)},
            'Face creams and masks': {'count': 23, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (5.0, 20.0), 'weight_range': (50, 250)},
            'Cleansing and makeup remover waters': {'count': 29, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (3.0, 8.0), 'weight_options': [150, 250, 500]},
            'Acetone': {'count': 2, 'store_max_stock_range': (15, 30), 'warehouse_max_stock_range': (150, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 3.0), 'weight_options': [100, 200]},
            'Pre-shave': {'count': 17, 'store_max_stock_range': (15, 35), 'warehouse_max_stock_range': (150, 350), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 6.0), 'weight_range': (50, 200)},
            'Razors and men\'s accessories': {'count': 39, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.5, 10.0), 'weight_range': (50, 200)},
            'Razors and women\'s accessories': {'count': 33, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.5, 10.0), 'weight_range': (50, 200)},
            'Aftershave': {'count': 15, 'store_max_stock_range': (15, 35), 'warehouse_max_stock_range': (150, 350), 'current_stock_percent': (0.7, 1.0), 'price_range': (3.0, 8.0), 'weight_range': (50, 250)},
            'Band-aids and pharmaceuticals': {'count': 14, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 5.0), 'weight_range': (20, 100)},
            'Trash bags': {'count': 18, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 6.0), 'weight_range': (250, 1000)},
        }
    },
    'LAUNDRY PRODUCTS': {
        'aisle': 8,
        'perishable': False,
        'subcategories': {
            'Hand wash': {'count': 7, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (100, 250), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_options': [500, 1000, 1500]},
            'Liquid detergent': {'count': 42, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (4.0, 12.0), 'weight_options': [1000, 2000, 4000]},
            'Powder and sheet detergent': {'count': 20, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (3.5, 10.0), 'weight_options': [500, 1000, 2000]},
            'Descalers': {'count': 11, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.5, 6.0), 'weight_options': [250, 500, 750]},
            'Fabric softeners': {'count': 36, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (3.0, 8.0), 'weight_options': [750, 1000, 2000]},
            'Laundry scents': {'count': 19, 'store_max_stock_range': (15, 30), 'warehouse_max_stock_range': (150, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_options': [150, 250, 500]},
            'Bleaches': {'count': 28, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [1000, 2000, 3000]},
            'Stain removers': {'count': 19, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.5, 7.0), 'weight_options': [250, 500, 750]},
        }
    },
    'HOUSE CLEANING PRODUCTS': {
        'aisle': 9,
        'perishable': False,
        'subcategories': {
            'Bleach': {'count': 15, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [1000, 2000, 3000]},
            'Dish detergents': {'count': 38, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 5.0), 'weight_options': [250, 500, 1000, 1500]},
            'Dishwasher detergents': {'count': 43, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (5.0, 15.0), 'weight_options': [500, 1000, 2000]},
            'Dishwasher salt': {'count': 2, 'store_max_stock_range': (10, 20), 'warehouse_max_stock_range': (100, 200), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [500, 1000]},
            'Ironing products': {'count': 4, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (100, 250), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_options': [250, 500, 750]},
            'Bathroom cleaners': {'count': 26, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 6.0), 'weight_options': [500, 750, 1000]},
            'Drain cleaners': {'count': 8, 'store_max_stock_range': (10, 20), 'warehouse_max_stock_range': (100, 200), 'current_stock_percent': (0.7, 1.0), 'price_range': (3.0, 8.0), 'weight_options': [500, 1000]},
            'Air fresheners': {'count': 21, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_range': (100, 300)},
            'Anti-limescale': {'count': 7, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (100, 250), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.5, 6.0), 'weight_options': [500, 1000]},
            'Detergents and disinfectants': {'count': 31, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (3.0, 8.0), 'weight_options': [500, 1000, 1500]},
            'Glass and furniture cleaners': {'count': 28, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_options': [500, 1000]},
            'Insecticides': {'count': 34, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (3.0, 7.0), 'weight_range': (250, 750)},
            'Gloves': {'count': 25, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_range': (50, 200)},
            'Sponges and cloths': {'count': 41, 'store_max_stock_range': (15, 50), 'warehouse_max_stock_range': (150, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 5.0), 'weight_range': (50, 250)},
        }
    },
    'BABY PRODUCTS': {
        'aisle': 10,
        'perishable': False,
        'subcategories': {
            'Diapers': {'count': 42, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (5.0, 15.0), 'weight_range': (500, 2000)},
            'Cookies': {'count': 7, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [100, 200, 400]},
            'Pasta and creams': {'count': 30, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_options': [150, 250, 500]},
            'Baby formula': {'count': 23, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (8.0, 20.0), 'weight_options': [400, 800, 1000]},
            'Homogenized foods': {'count': 81, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (0.8, 3.0), 'weight_options': [80, 100, 150]},
            'Care and hygiene': {'count': 29, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.5, 8.0), 'weight_range': (100, 500)},
        }
    },
    'FRUITS AND VEGETABLES': {
        'aisle': 11,
        'perishable': True,
        'subcategories': {
            'Apples': {'count': 7, 'store_max_stock_range': (100, 300), 'warehouse_max_stock_range': (80, 250), 'current_stock_probabilities': [0], 'price_range': (1.5, 3.0), 'weight_range': (100, 300)},
            'Pears': {'count': 5, 'store_max_stock_range': (100, 300), 'warehouse_max_stock_range': (80, 250), 'current_stock_probabilities': [0], 'price_range': (2.0, 3.5), 'weight_range': (100, 300)},
            'Cherries': {'count': 3, 'store_max_stock_range': (50, 150), 'warehouse_max_stock_range': (40, 120), 'current_stock_probabilities': [0], 'price_range': (5.0, 10.0), 'weight_range': (10, 50)},
            'Strawberries': {'count': 2, 'store_max_stock_range': (50, 100), 'warehouse_max_stock_range': (40, 80), 'current_stock_probabilities': [0], 'price_range': (3.0, 6.0), 'weight_range': (10, 50)},
            'Apricots': {'count': 3, 'store_max_stock_range': (50, 150), 'warehouse_max_stock_range': (40, 120), 'current_stock_probabilities': [0], 'price_range': (2.5, 5.0), 'weight_range': (20, 75)},
            'Plums': {'count': 3, 'store_max_stock_range': (50, 150), 'warehouse_max_stock_range': (40, 120), 'current_stock_probabilities': [0], 'price_range': (2.0, 4.0), 'weight_range': (30, 100)},
            'Yellow Peaches': {'count': 4, 'store_max_stock_range': (50, 150), 'warehouse_max_stock_range': (40, 120), 'current_stock_probabilities': [0], 'price_range': (2.5, 5.0), 'weight_range': (100, 300)},
            'Grapes': {'count': 3, 'store_max_stock_range': (50, 150), 'warehouse_max_stock_range': (40, 120), 'current_stock_probabilities': [0], 'price_range': (3.0, 6.0), 'weight_range': (100, 300)},
            'Bananas': {'count': 3, 'store_max_stock_range': (100, 300), 'warehouse_max_stock_range': (80, 250), 'current_stock_probabilities': [0], 'price_range': (1.0, 2.5), 'weight_range': (100, 250)},
            'Kiwi': {'count': 2, 'store_max_stock_range': (50, 150), 'warehouse_max_stock_range': (40, 120), 'current_stock_probabilities': [0], 'price_range': (2.0, 4.0), 'weight_range': (50, 150)},
            'Pineapples': {'count': 2, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (8, 25), 'current_stock_probabilities': [0], 'price_range': (2.5, 5.0), 'weight_range': (500, 2000)},
            'Avocados': {'count': 2, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (15, 40), 'current_stock_probabilities': [0], 'price_range': (1.5, 4.0), 'weight_range': (100, 300)},
            'Watermelons': {'count': 1, 'store_max_stock_range': (5, 10), 'warehouse_max_stock_range': (4, 8), 'current_stock_probabilities': [0], 'price_range': (0.8, 1.5), 'weight_range': (2000, 8000)},
            'Melons': {'count': 2, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (8, 20), 'current_stock_probabilities': [0], 'price_range': (1.0, 2.5), 'weight_range': (1000, 4000)},
            'Berries': {'count': 6, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (15, 45), 'current_stock_probabilities': [0], 'price_range': (2.0, 5.0), 'weight_range': (50, 250)},
            'Potatoes': {'count': 5, 'store_max_stock_range': (50, 200), 'warehouse_max_stock_range': (40, 150), 'current_stock_probabilities': [0], 'price_range': (0.8, 2.0), 'weight_range': (500, 3000)},
            'Onions': {'count': 4, 'store_max_stock_range': (50, 200), 'warehouse_max_stock_range': (40, 150), 'current_stock_probabilities': [0], 'price_range': (0.5, 1.5), 'weight_range': (250, 1000)},
            'Spring onions': {'count': 3, 'store_max_stock_range': (30, 80), 'warehouse_max_stock_range': (25, 60), 'current_stock_probabilities': [0], 'price_range': (1.0, 2.0), 'weight_range': (50, 200)},
            'Carrots': {'count': 4, 'store_max_stock_range': (50, 150), 'warehouse_max_stock_range': (40, 120), 'current_stock_probabilities': [0], 'price_range': (0.8, 2.5), 'weight_range': (250, 1000)},
            'Radishes': {'count': 2, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (15, 45), 'current_stock_probabilities': [0], 'price_range': (1.0, 2.0), 'weight_range': (50, 200)},
            'Green beans': {'count': 2, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (15, 45), 'current_stock_probabilities': [0], 'price_range': (2.0, 4.0), 'weight_range': (250, 500)},
            'Tomatoes': {'count': 10, 'store_max_stock_range': (50, 150), 'warehouse_max_stock_range': (40, 120), 'current_stock_probabilities': [0], 'price_range': (1.5, 3.5), 'weight_range': (100, 500)},
            'Cucumbers': {'count': 1, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (15, 40), 'current_stock_probabilities': [0], 'price_range': (0.8, 2.0), 'weight_range': (150, 500)},
            'Peppers': {'count': 3, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (15, 45), 'current_stock_probabilities': [0], 'price_range': (2.0, 4.0), 'weight_range': (150, 500)},
            'Eggplants': {'count': 1, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (12, 30), 'current_stock_probabilities': [0], 'price_range': (1.5, 3.0), 'weight_range': (250, 750)},
            'Zucchini': {'count': 2, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (15, 40), 'current_stock_probabilities': [0], 'price_range': (1.0, 3.0), 'weight_range': (150, 500)},
            'Fennels': {'count': 2, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (12, 30), 'current_stock_probabilities': [0], 'price_range': (1.5, 3.0), 'weight_range': (250, 750)},
            'Celery': {'count': 2, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (12, 30), 'current_stock_probabilities': [0], 'price_range': (1.0, 2.5), 'weight_range': (250, 750)},
            'Cabbages': {'count': 4, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (8, 20), 'current_stock_probabilities': [0], 'price_range': (1.0, 2.5), 'weight_range': (500, 2000)},
            'Broccoli': {'count': 2, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (12, 30), 'current_stock_probabilities': [0], 'price_range': (1.5, 3.5), 'weight_range': (250, 750)},
            'Pumpkins': {'count': 2, 'store_max_stock_range': (5, 15), 'warehouse_max_stock_range': (4, 12), 'current_stock_probabilities': [0], 'price_range': (0.8, 2.0), 'weight_range': (1000, 5000)},
            'Beets and chicory': {'count': 2, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (12, 30), 'current_stock_probabilities': [0], 'price_range': (1.5, 3.0), 'weight_range': (250, 750)},
            'Lettuce': {'count': 3, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (15, 40), 'current_stock_probabilities': [0], 'price_range': (1.0, 2.5), 'weight_range': (200, 500)},
            'Radicchio': {'count': 1, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (15, 40), 'current_stock_probabilities': [0], 'price_range': (1.5, 3.0), 'weight_range': (200, 500)},
            'Oranges': {'count': 3, 'store_max_stock_range': (100, 300), 'warehouse_max_stock_range': (80, 250), 'current_stock_probabilities': [0], 'price_range': (1.0, 2.5), 'weight_range': (100, 300)},
            'Mandarins': {'count': 3, 'store_max_stock_range': (100, 300), 'warehouse_max_stock_range': (80, 250), 'current_stock_probabilities': [0], 'price_range': (1.5, 3.0), 'weight_range': (50, 150)},
            'Grapefruits': {'count': 1, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (15, 40), 'current_stock_probabilities': [0], 'price_range': (1.0, 2.0), 'weight_range': (200, 600)},
            'Ready-to-eat raw vegetables': {'count': 37, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (15, 45), 'current_stock_probabilities': [0], 'price_range': (2.0, 5.0), 'weight_range': (100, 500)},
            'Ready-to-eat cooked vegetables': {'count': 10, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (12, 30), 'current_stock_probabilities': [0], 'price_range': (2.5, 6.0), 'weight_range': (200, 750)},
            'Soups and side dishes': {'count': 39, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_options': [250, 500, 1000]},
            'Garlic and chilies': {'count': 4, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (15, 40), 'current_stock_probabilities': [0], 'price_range': (0.5, 2.0), 'weight_range': (50, 250)},
            'Leaf herbs': {'count': 14, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (15, 40), 'current_stock_probabilities': [0], 'price_range': (1.0, 3.0), 'weight_range': (20, 100)},
        }
    },
    'LONG-LIFE FRUIT': {
        'aisle': 12,
        'subcategories': {
            'Canned fruit': {'count': 11, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [200, 400, 800]},
            'Fruit desserts': {'count': 16, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [100, 125, 250]},
            'Dried fruit': {'count': 21, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 6.0), 'weight_options': [50, 100, 250]},
            'Nuts': {'count': 32, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (3.0, 10.0), 'weight_options': [100, 250, 500]},
            'Other snacks': {'count': 43, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 4.0), 'weight_range': (50, 250)},
            'Dried legumes': {'count': 20, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [250, 500, 1000]},
            'Seeds': {'count': 7, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [50, 100, 250]},
        }
    },
    'SWEETS AND BREAKFAST': {
        'aisle': 13,
        'subcategories': {
            'Snack cakes': {'count': 97, 'store_max_stock_range': (30, 80), 'warehouse_max_stock_range': (300, 800), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_range': (100, 500)},
            'Ready-made cakes': {'count': 19, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (100, 250), 'current_stock_percent': (0.7, 1.0), 'price_range': (3.0, 10.0), 'weight_range': (250, 1000)},
            'Cookies': {'count': 114, 'store_max_stock_range': (40, 100), 'warehouse_max_stock_range': (400, 1000), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 5.0), 'weight_options': [250, 500, 1000]},
            'Industrial pastries': {'count': 63, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 6.0), 'weight_range': (200, 750)},
            'Snacks and bars': {'count': 71, 'store_max_stock_range': (30, 80), 'warehouse_max_stock_range': (300, 800), 'current_stock_percent': (0.7, 1.0), 'price_range': (0.8, 3.0), 'weight_range': (20, 200)},
            'Sweeteners': {'count': 12, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 6.0), 'weight_range': (50, 250)},
            'Candies': {'count': 58, 'store_max_stock_range': (40, 100), 'warehouse_max_stock_range': (400, 1000), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 4.0), 'weight_range': (50, 500)},
            'Flavors and decorations': {'count': 31, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_range': (20, 150)},
            'Cocoa powder': {'count': 5, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_options': [100, 250, 500]},
            'Puddings': {'count': 13, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [50, 100, 200]},
            'Spreads': {'count': 25, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.5, 7.0), 'weight_options': [200, 400, 750]},
            'Chocolate bars': {'count': 32, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 4.0), 'weight_options': [50, 100, 200]},
            'Chocolates': {'count': 27, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 8.0), 'weight_range': (50, 300)},
            'Chocolate eggs': {'count': 10, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 5.0), 'weight_range': (50, 200)},
            'Cereals': {'count': 38, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 6.0), 'weight_options': [250, 500, 1000]},
            'Rusks': {'count': 36, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [150, 300, 500]},
            'Milk modifiers and chocolate preparations': {'count': 12, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_options': [100, 200, 400]},
            'Jams': {'count': 52, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 5.0), 'weight_options': [200, 350, 500]},
            'Honey': {'count': 11, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (3.0, 8.0), 'weight_options': [250, 500]},
        }
    },
    'FLOURS AND SUGAR': {
        'aisle': 14,
        'subcategories': {
            'Sugar': {'count': 13, 'store_max_stock_range': (20, 80), 'warehouse_max_stock_range': (200, 800), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [500, 1000]},
            'Flour': {'count': 44, 'store_max_stock_range': (50, 100), 'warehouse_max_stock_range': (500, 1000), 'current_stock_percent': (0.7, 1.0), 'price_range': (0.8, 2.5), 'weight_options': [500, 1000, 2000]},
            'Corn flour and polenta': {'count': 18, 'store_max_stock_range': (30, 80), 'warehouse_max_stock_range': (300, 800), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [500, 1000]},
            'Yeast': {'count': 21, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (0.5, 2.0), 'weight_range': (10, 50)},
            'Salt': {'count': 14, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (0.5, 2.0), 'weight_options': [250, 500, 1000]},
        }
    },
    'PASTA AND RICE': {
        'aisle': 15,
        'subcategories': {
            'Rice': {'count': 43, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 5.0), 'weight_options': [500, 1000]},
            'Spelt and whole wheat pasta': {'count': 23, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.2, 3.0), 'weight_options': [250, 500]},
            'Cereals and others': {'count': 18, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [250, 500, 1000]},
            'Pasta': {'count': 98, 'store_max_stock_range': (30, 80), 'warehouse_max_stock_range': (300, 800), 'current_stock_percent': (0.7, 1.0), 'price_range': (0.8, 2.5), 'weight_options': [500, 1000]},
            'Egg pasta': {'count': 47, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [250, 500]},
            'Soup and risotto mixes': {'count': 45, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.5), 'weight_options': [150, 250, 500]},
            'Stock cubes and broths': {'count': 31, 'store_max_stock_range': (30, 80), 'warehouse_max_stock_range': (300, 800), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_range': (50, 250)},
        }
    },
    'GLUTEN FREE': {
        'aisle': 16,
        'subcategories': {
            'Pasta': {'count': 21, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_options': [250, 500]},
            'Baked goods': {'count': 4, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (100, 250), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.5, 6.0), 'weight_options': [100, 250, 400]},
            'Cookies': {'count': 18, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_options': [100, 250, 500]},
            'Rice cakes': {'count': 4, 'store_max_stock_range': (15, 30), 'warehouse_max_stock_range': (150, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [50, 100, 200]},
            'Savory snacks': {'count': 13, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_range': (50, 250)},
            'Sweet snacks': {'count': 23, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_range': (50, 250)},
        }
    },
    'BREAD AND BREADSTICKS': {
        'aisle': 17,
        'subcategories': {
            'Breadsticks': {'count': 29, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [100, 200, 400]},
            'Flatbreads': {'count': 7, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (100, 250), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [100, 250, 500]},
            'Rice cakes': {'count': 29, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [50, 100, 200]},
            'Packaged bread': {'count': 53, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 4.0), 'weight_range': (250, 750)},
            'Long-life flatbreads': {'count': 13, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [200, 300, 600]},
            'Breadcrumbs': {'count': 3, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (0.8, 2.5), 'weight_options': [250, 500]},
            'Pizza and bread mixes': {'count': 8, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [250, 500]},
            'Crackers': {'count': 25, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [100, 200, 400]},
            'Taralli': {'count': 15, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [100, 250, 500]},
        }
    },
    'HOT BEVERAGES': {
        'aisle': 18,
        'subcategories': {
            'Ground coffee': {'count': 28, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 6.0), 'weight_options': [100, 250, 500]},
            'Decaf ground coffee': {'count': 6, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.5, 7.0), 'weight_options': [100, 250]},
            'Coffee pods and capsules': {'count': 46, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (3.0, 10.0), 'weight_range': (50, 200)},
            'Coffee beans': {'count': 10, 'store_max_stock_range': (15, 30), 'warehouse_max_stock_range': (150, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (3.0, 8.0), 'weight_options': [250, 500, 1000]},
            'Instant coffee': {'count': 21, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.5, 7.0), 'weight_options': [50, 100, 250]},
            'Barley coffee': {'count': 10, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_options': [100, 250, 500]},
            'Tea bags': {'count': 32, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 5.0), 'weight_range': (20, 150)},
            'Decaf tea bags': {'count': 8, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 4.5), 'weight_range': (15, 100)},
            'Instant chamomile': {'count': 6, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (100, 250), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 3.5), 'weight_options': [50, 100, 200]},
            'Chamomile tea bags': {'count': 8, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_range': (20, 100)},
            'Other infusions': {'count': 34, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.5), 'weight_range': (20, 150)},
        }
    },
    'SAUCES': {
        'aisle': 19,
        'subcategories': {
            'Mayonnaise': {'count': 21, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [150, 250, 500]},
            'Ketchup': {'count': 13, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [250, 500, 750]},
            'Mustard': {'count': 8, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [150, 250, 500]},
            'Savory spreads': {'count': 14, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_options': [100, 200, 400]},
            'Other sauces': {'count': 30, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 5.0), 'weight_options': [150, 250, 500]},
        }
    },
    'PASTA SAUCES': {
        'aisle': 20,
        'subcategories': {
            'Ready-made sauces': {'count': 60, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 5.0), 'weight_options': [200, 400, 750]},
            'Tomato puree': {'count': 30, 'store_max_stock_range': (25, 60), 'warehouse_max_stock_range': (250, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (0.8, 2.5), 'weight_options': [400, 700, 1000]},
            'Tomato pulp': {'count': 21, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (0.8, 2.0), 'weight_options': [200, 400, 500]},
        }
    },
    'SPICES': {
        'aisle': 21,
        'subcategories': {
            'Spices': {'count': 19, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 5.0), 'weight_range': (10, 50)},
            'Herbs': {'count': 21, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 4.0), 'weight_range': (10, 50)},
            'Flavorings': {'count': 16, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_range': (50, 200)},
        }
    },
    'VINEGAR AND OIL': {
        'aisle': 22,
        'subcategories': {
            'White vinegar': {'count': 10, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 2.5), 'weight_options': [500, 750, 1000]},
            'Balsamic vinegar': {'count': 9, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 10.0), 'weight_options': [250, 500, 750]},
            'Vinegar-condiments': {'count': 12, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 5.0), 'weight_options': [250, 500, 750]},
            'Olive oil': {'count': 46, 'store_max_stock_range': (6, 15), 'warehouse_max_stock_range': (60, 150), 'current_stock_percent': (0.7, 1.0), 'price_range': (5.0, 20.0), 'weight_options': [500, 750, 1000, 2000]},
            'Corn oil': {'count': 5, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (100, 250), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_options': [500, 1000, 1500]},
            'Peanut oil': {'count': 4, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (100, 250), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.5, 6.0), 'weight_options': [500, 1000, 1500]},
            'Soybean oil': {'count': 2, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (100, 250), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_options': [500, 1000, 1500]},
            'Sunflower oil': {'count': 6, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (100, 250), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [500, 1000, 1500]},
            'Frying oil': {'count': 4, 'store_max_stock_range': (10, 20), 'warehouse_max_stock_range': (100, 200), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.5, 6.0), 'weight_options': [500, 1000, 1500]},
        }
    },
    'SAVORY SNACKS': {
        'aisle': 23,
        'subcategories': {
            'Chips and similar': {'count': 64, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 4.0), 'weight_range': (50, 300)},
            'Popcorn': {'count': 8, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [100, 250, 500]},
            'Toasted snacks': {'count': 18, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_range': (50, 250)},
        }
    },
    'PET PRODUCTS': {
        'aisle': 24,
        'subcategories': {
            'Wet cat food': {'count': 57, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (0.5, 2.0), 'weight_options': [50, 85, 100, 200]},
            'Dry cat food': {'count': 32, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 10.0), 'weight_options': [250, 500, 1000, 2000]},
            'Cat snacks': {'count': 15, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_range': (20, 150)},
            'Wet dog food': {'count': 41, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [100, 200, 400, 500]},
            'Dry dog food': {'count': 30, 'store_max_stock_range': (15, 30), 'warehouse_max_stock_range': (150, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (3.0, 15.0), 'weight_options': [500, 1000, 2000, 5000]},
            'Dog hygiene and care': {'count': 7, 'store_max_stock_range': (10, 20), 'warehouse_max_stock_range': (100, 200), 'current_stock_percent': (0.7, 1.0), 'price_range': (3.0, 8.0), 'weight_range': (100, 500)},
            'Cat litter': {'count': 10, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (100, 250), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.5, 7.0), 'weight_options': [1000, 2000, 5000]},
        }
    },
    'CANNED GOODS': {
        'aisle': 25,
        'subcategories': {
            'Peeled tomatoes': {'count': 7, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (0.8, 2.0), 'weight_options': [200, 400, 500]},
            'Peas': {'count': 9, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 2.5), 'weight_options': [200, 300, 400]},
            'Beans': {'count': 16, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (0.8, 2.0), 'weight_options': [200, 400]},
            'Corn': {'count': 9, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 2.5), 'weight_options': [150, 300]},
            'Other legumes and vegetables': {'count': 23, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [200, 400, 500]},
            'Vegetables in oil': {'count': 41, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 6.0), 'weight_options': [150, 250, 500]},
            'Vegetables in vinegar': {'count': 12, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [200, 300, 500]},
            'Sweet and sour vegetables': {'count': 8, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [200, 300, 500]},
            'Olives': {'count': 32, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 5.0), 'weight_options': [100, 200, 400]},
            'Condiriso': {'count': 11, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [250, 500]},
            'Capers': {'count': 8, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [50, 100, 200]},
            'Dried mushrooms': {'count': 5, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_range': (100, 250), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 6.0), 'weight_options': [20, 50, 100]},
            'Canned meat': {'count': 12, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [50, 100, 250]},
            'Tuna in oil': {'count': 51, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_range': (200, 600), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 3.0), 'weight_options': [80, 120, 160, 200]},
            'Tuna in water': {'count': 14, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_range': (200, 500), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.0, 2.5), 'weight_options': [80, 120, 200]},
            'Tuna with other condiments': {'count': 20, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 3.5), 'weight_options': [80, 120, 200]},
            'Mackerel and sardines': {'count': 22, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_range': (150, 400), 'current_stock_percent': (0.7, 1.0), 'price_range': (1.5, 4.0), 'weight_options': [80, 120, 200]},
            'Anchovies and similar': {'count': 20, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 5.0), 'weight_options': [50, 100, 150]},
            'Other fish products': {'count': 10, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_range': (100, 300), 'current_stock_percent': (0.7, 1.0), 'price_range': (2.0, 6.0), 'weight_range': (50, 200)},
        }
    },
    'REFRIGERATED': {
        'aisle': 26,
        'subcategories': {
            'Yogurt': {'count': 60, 'store_max_stock_range': (30, 80), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (0.8, 3.0), 'weight_options': [125, 150, 200, 500]},
            'Fresh Milk': {'count': 15, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (1.2, 2.5), 'weight_options': [500, 1000]},
            'Eggs': {'count': 10, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (2.0, 5.0), 'weight_range': (300, 1000)},
            'Fresh Pasta': {'count': 45, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (1.5, 5.0), 'weight_options': [250, 500]},
            'Butter and Margarine': {'count': 20, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (2.0, 6.0), 'weight_options': [125, 250, 500]},
            'Cheese and Cured Meats': {'count': 120, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (3.0, 20.0), 'weight_range': (100, 1000)},
            'Pies and Pizza dough': {'count': 25, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (1.5, 4.0), 'weight_options': [200, 300, 500]},
            'Fresh Juices': {'count': 15, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (2.5, 5.0), 'weight_options': [250, 500, 1000]},
        }
    },
    'FROZEN': {
        'aisle': 27,
        'subcategories': {
            'Frozen Vegetables': {'count': 40, 'store_max_stock_range': (20, 60), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (1.5, 4.0), 'weight_options': [250, 450, 750, 1000]},
            'Frozen Fish': {'count': 25, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (3.0, 10.0), 'weight_range': (200, 750)},
            'Frozen Potatoes': {'count': 10, 'store_max_stock_range': (20, 50), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (1.5, 3.5), 'weight_options': [500, 750, 1000, 1500]},
            'Ice Creams': {'count': 50, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (2.0, 8.0), 'weight_range': (200, 1000)},
            'Pizzas': {'count': 30, 'store_max_stock_range': (10, 25), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (2.5, 6.0), 'weight_options': [300, 400, 800]},
            'Frozen Ready Meals': {'count': 35, 'store_max_stock_range': (15, 40), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (3.0, 8.0), 'weight_range': (250, 750)},
        }
    },
    'MEAT': {
        'aisle': 28,
        'subcategories': {
            'Beef': {'count': 40, 'store_max_stock_range': (5, 15), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (8.0, 30.0), 'weight_range': (200, 1000)},
            'Pork': {'count': 30, 'store_max_stock_range': (5, 15), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (6.0, 18.0), 'weight_range': (200, 1000)},
            'Chicken': {'count': 50, 'store_max_stock_range': (8, 20), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (5.0, 15.0), 'weight_range': (200, 1500)},
            'Sausages and Burgers': {'count': 25, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (4.0, 12.0), 'weight_options': [250, 400, 500, 750]},
            'Cold Cuts': {'count': 70, 'store_max_stock_range': (10, 30), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (2.0, 15.0), 'weight_range': (100, 500)},
        }
    },
    'FISH': {
        'aisle': 29,
        'subcategories': {
            'Fresh Fish': {'count': 30, 'store_max_stock_range': (5, 10), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (10.0, 40.0), 'weight_range': (200, 1500)},
            'Shellfish': {'count': 15, 'store_max_stock_range': (5, 10), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (15.0, 50.0), 'weight_range': (200, 1000)},
            'Smoked Salmon': {'count': 10, 'store_max_stock_range': (10, 20), 'warehouse_max_stock_equal_store': True, 'current_stock_probabilities': [0], 'price_range': (8.0, 25.0), 'weight_options': [100, 200, 500]},
        }
    }
}

# Function to generate the product catalog DataFrame

def generate_product_catalog(product_hierarchy):
    catalog_data = []
    shelf_id_counters = {cat: {sub: 0 for sub in cat_props['subcategories'].keys()} 
                         for cat, cat_props in product_hierarchy.items()}

    for category, cat_props in product_hierarchy.items():
        aisle = cat_props['aisle']
        for subcategory, sub_props in cat_props['subcategories'].items():
            for i in range(sub_props['count']):
                shelf_id_counters[category][subcategory] += 1
                
                shelf_id_prefix = category.replace(' ', '')[:3].upper()
                subcategory_prefix = subcategory.replace(' ', '')[:3].upper()
                shelf_id = f"{shelf_id_prefix}{subcategory_prefix}{shelf_id_counters[category][subcategory]}"

                if 'weight_options' in sub_props:
                    item_weight = np.random.choice(sub_props['weight_options'])
                else:
                    item_weight = np.random.randint(sub_props['weight_range'][0], sub_props['weight_range'][1])
                
                item_price = round(np.random.uniform(sub_props['price_range'][0], sub_props['price_range'][1]), 2)
                
                catalog_data.append({
                    'shelf_id': shelf_id,
                    'aisle': aisle,
                    'item_category': category,
                    'item_subcategory': subcategory,
                    'item_weight': item_weight,
                    'item_price': item_price
                })
    return pd.DataFrame(catalog_data)


# Function to generate inventory DataFrame

def generate_inventory_df(product_catalog, product_hierarchy, inventory_type, store_max_stock_data=None):
    data = []
    
    for _, product_row in product_catalog.iterrows():
        shelf_id = product_row['shelf_id']
        aisle = product_row['aisle']
        category = product_row['item_category']
        subcategory = product_row['item_subcategory']
        item_weight = product_row['item_weight']
        item_price = product_row['item_price']
        
        cat_props = product_hierarchy[category]
        sub_props = cat_props['subcategories'][subcategory]
        is_perishable = 'current_stock_probabilities' in sub_props

        # Logic for maximum stock of warehouse and store
        if inventory_type == 'warehouse':
            if 'warehouse_max_stock_equal_store' in sub_props and sub_props['warehouse_max_stock_equal_store']:
                maximum_stock = store_max_stock_data.loc[store_max_stock_data['shelf_id'] == shelf_id, 'maximum_stock'].iloc[0]
            elif 'warehouse_max_stock_range' in sub_props:
                maximum_stock = np.random.randint(sub_props['warehouse_max_stock_range'][0], sub_props['warehouse_max_stock_range'][1])
            else:
                store_max = store_max_stock_data.loc[store_max_stock_data['shelf_id'] == shelf_id, 'maximum_stock'].iloc[0]
                maximum_stock = int(np.random.uniform(0.1, 0.4) * store_max)
                maximum_stock = max(1, maximum_stock)
        else:
            maximum_stock = np.random.randint(sub_props['store_max_stock_range'][0], sub_props['store_max_stock_range'][1])
        
        # Logic for current stock based on maximum stock and perishability
        if inventory_type == 'warehouse' and is_perishable:
            current_stock = 0
        elif is_perishable:
            current_stock = np.random.choice(sub_props['current_stock_probabilities'])
            current_stock = min(current_stock, maximum_stock)
        else:
            current_stock_ratio = np.random.uniform(sub_props['current_stock_percent'][0], sub_props['current_stock_percent'][1])
            current_stock = int(maximum_stock * current_stock_ratio)

        shelf_weight = item_weight * maximum_stock
        item_visibility = round(np.random.uniform(0.05, 0.2), 6) if inventory_type == 'store' else None
        time_stamp = datetime.now() - timedelta(minutes=np.random.randint(0, 10))

        row = {
            'shelf_id': shelf_id,
            'aisle': aisle,
            'item_weight': item_weight,
            'shelf_weight': shelf_weight,
            'item_category': category,
            'item_subcategory': subcategory,
            'maximum_stock': maximum_stock,
            'current_stock': current_stock,
            'item_price': item_price,
            'time_stamp': time_stamp.strftime('%Y-%m-%d %H:%M:%S')
        }
        if inventory_type == 'store':
            row['item_visibility'] = item_visibility
        
        data.append(row)

    df = pd.DataFrame(data)
    
    if inventory_type == 'store':
        cols = ['shelf_id', 'aisle', 'item_weight', 'shelf_weight', 'item_category', 'item_subcategory', 
                'item_visibility', 'maximum_stock', 'current_stock', 'item_price', 'time_stamp']
    else:
        cols = ['shelf_id', 'aisle', 'item_weight', 'shelf_weight', 'item_category', 'item_subcategory', 
                'maximum_stock', 'current_stock', 'item_price', 'time_stamp']
    
    return df[cols]

# Function to generate batches data DataFrame

def generate_batches_data(inventory_df, product_hierarchy, location_type):
    batches_data = []
    
    for _, row in inventory_df.iterrows():
        shelf_id = row['shelf_id']
        current_stock = row['current_stock']
        category = row['item_category']
        subcategory = row['item_subcategory']
        
        # Skip if current stock is zero
        if current_stock == 0:
            continue

        # Decide number of batches 
        if current_stock > 1:
            num_batches = np.random.choice([1, 2], p=[0.7, 0.3])
        else:
             num_batches = 1

        # Generate batch data
        if num_batches == 1:
            batch_qty_store = current_stock if location_type == 'in-store' else 0
            batch_qty_warehouse = current_stock if location_type == 'warehouse' else 0
            
            is_perishable = 'current_stock_probabilities' in product_hierarchy[category]['subcategories'][subcategory]

            if is_perishable:
                expiry_delta = timedelta(days=np.random.randint(2, 15))
            else:
                expiry_delta = timedelta(days=np.random.randint(365, 730))
            
            batches_data.append({
                'shelf_id': shelf_id,
                'batch_code': f'B-{np.random.randint(1000, 9999)}',
                'item_category': category,
                'item_subcategory': subcategory,
                'received_date': (datetime.now() - timedelta(days=np.random.randint(0, 3))).strftime('%Y-%m-%d'),
                'expiry_date': (datetime.now() + expiry_delta).strftime('%Y-%m-%d'),
                'batch_quantity_total': current_stock,
                'batch_quantity_store': batch_qty_store,
                'batch_quantity_warehouse': batch_qty_warehouse,
                'location': location_type,
                'time_stamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        else:
            split_point = np.random.randint(1, current_stock)
            
            batch1_qty_store = split_point if location_type == 'in-store' else 0
            batch1_qty_warehouse = split_point if location_type == 'warehouse' else 0
            
            batch2_qty_store = (current_stock - split_point) if location_type == 'in-store' else 0
            batch2_qty_warehouse = (current_stock - split_point) if location_type == 'warehouse' else 0
            
            # Determine perishability
            is_perishable = 'current_stock_probabilities' in product_hierarchy[category]['subcategories'][subcategory]

            if is_perishable:
                expiry_delta1 = timedelta(days=np.random.randint(2, 10))
                expiry_delta2 = timedelta(days=np.random.randint(8, 15))
            else:
                expiry_delta1 = timedelta(days=np.random.randint(365, 730))
                expiry_delta2 = timedelta(days=np.random.randint(730, 1095))

            batches_data.append({
                'shelf_id': shelf_id,
                'batch_code': f'B-{np.random.randint(1000, 9999)}',
                'item_category': category,
                'item_subcategory': subcategory,
                'received_date': (datetime.now() - timedelta(days=np.random.randint(4, 7))).strftime('%Y-%m-%d'),
                'expiry_date': (datetime.now() + expiry_delta1).strftime('%Y-%m-%d'),
                'batch_quantity_total': split_point,
                'batch_quantity_store': batch1_qty_store,
                'batch_quantity_warehouse': batch1_qty_warehouse,
                'location': location_type,
                'time_stamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            
            batches_data.append({
                'shelf_id': shelf_id,
                'batch_code': f'B-{np.random.randint(1000, 9999)}',
                'item_category': category,
                'item_subcategory': subcategory,
                'received_date': (datetime.now() - timedelta(days=np.random.randint(0, 3))).strftime('%Y-%m-%d'),
                'expiry_date': (datetime.now() + expiry_delta2).strftime('%Y-%m-%d'),
                'batch_quantity_total': current_stock - split_point,
                'batch_quantity_store': batch2_qty_store,
                'batch_quantity_warehouse': batch2_qty_warehouse,
                'location': location_type,
                'time_stamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
    
    return pd.DataFrame(batches_data)

# main

print("Generating product catalog...")
product_catalog = generate_product_catalog(product_hierarchy)

print("Generating store inventory...")
store_inventory_df = generate_inventory_df(product_catalog, product_hierarchy, 'store')
store_inventory_df.to_parquet('data/store_inventory_final.parquet', index=False)
print("Store inventory saved successfully!")

print("Generating warehouse inventory...")
warehouse_inventory_df = generate_inventory_df(product_catalog, product_hierarchy, 'warehouse', store_max_stock_data=store_inventory_df)
warehouse_inventory_df.to_parquet('data/warehouse_inventory_final.parquet', index=False)
print("Wharehouse inventory saved successfully!")

print("Generating batches dataset for the store...")
store_batches_df = generate_batches_data(store_inventory_df, product_hierarchy, 'in-store')
store_batches_df.to_parquet('data/store_batches.parquet', index=False)
print("Batches dataset for the store saved successfully!")

print("Generating batches dataset for the warehouse...")
warehouse_batches_df = generate_batches_data(warehouse_inventory_df, product_hierarchy, 'warehouse')
warehouse_batches_df.to_parquet('data/warehouse_batches.parquet', index=False)
print("Batches dataset for the warehouse saved successfully!")

print("\n All datasets generated and saved successfully!")