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

# -------------------------------------------------------------------
# Helper: compute a realistic standard batch size per product
# -------------------------------------------------------------------

def compute_standard_batch_size(category, subcategory, item_weight):
    """
    Ritorna una dimensione standard del lotto (numero di pezzi)
    coerente con il tipo di prodotto e il suo peso.
    """
    # Beverage-like: multiple of 6
    beverage_like_categories = {
        'BEVERAGES', 'BEERS', 'WINES', 'LONG-LIFE DAIRY',
        'HOT BEVERAGES', 'VINEGAR AND OIL'
    }

    # Perishable categories (they usually have smaller batch sizes)
    perishable_categories = {
        'FRUITS AND VEGETABLES',
        'REFRIGERATED',
        'FROZEN',
        'MEAT',
        'FISH'
    }

    # -------------------------------
    # 1) Beverage-like products
    # -------------------------------
    if category in beverage_like_categories:
        # tipicamente pacchi da 6, 12 o 24
        return int(np.random.choice([18, 24, 30, 36, 42, 48, 60]))

    # -------------------------------
    # 2) Perishable products
    # -------------------------------
    if category in perishable_categories: 

        # Frutta e verdura: lotti medio-piccoli, non 200 pezzi
        if category == 'FRUITS AND VEGETABLES':
            if item_weight <= 80:          # ciliegie, frutti piccoli
                return int(np.random.choice([20, 25, 30, 35, 40]))
            elif item_weight <= 300:       # mele, arance, ecc.
                return int(np.random.choice([8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]))
            elif item_weight <= 1000:      # verdura medio-grande
                return int(np.random.choice([8, 9, 10, 11, 12]))
            else:                          # angurie, zucche, meloni...
                return int(np.random.choice([8, 9, 10, 11, 12]))
        # 2b) Altri deperibili (frigo, surgelato, carne, pesce)
        #    → lotti piccoli, per non riempire troppo il magazzino
        if item_weight <= 300:
            return int(np.random.choice([15, 20, 25, 30, 35, 40]))
        elif item_weight <= 800:
            return int(np.random.choice([8, 10, 11, 12, 14, 15, 16, 18, 20]))
        elif item_weight <= 1500:
            return int(np.random.choice([8, 10, 11, 12, 14, 15]))
        else:
            return int(np.random.choice([5, 6, 8, 10, 12]))

    # Default per the rest of products 
    if item_weight <= 200:
        return int(np.random.choice([20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80]))
    if item_weight <= 500:
        return int(np.random.choice([20, 25, 30, 35, 40, 45, 50, 55, 60]))
    elif item_weight <= 1000:
        return int(np.random.choice([15, 20, 25, 30, 35, 40]))
    elif item_weight <= 2000:
        return int(np.random.choice([10, 15, 20, 25, 30]))
    else:
        return int(np.random.choice([5, 10, 15, 20]))

# -------------------------------------------------------------------
# Product catalog generation
# -------------------------------------------------------------------
def generate_product_catalog(product_hierarchy):
    catalog_data = []
    # Counter to generate unique shelf_ids per (category, subcategory)
    shelf_id_counters = {
        cat: {sub: 0 for sub in cat_props['subcategories'].keys()}
        for cat, cat_props in product_hierarchy.items()
    }

    for category, cat_props in product_hierarchy.items():
        aisle = cat_props['aisle']
        for subcategory, sub_props in cat_props['subcategories'].items():
            for i in range(sub_props['count']):
                shelf_id_counters[category][subcategory] += 1

                shelf_id_prefix = category.replace(' ', '')[:3].upper()
                subcategory_prefix = subcategory.replace(' ', '')[:3].upper()
                shelf_id = f"{shelf_id_prefix}{subcategory_prefix}{shelf_id_counters[category][subcategory]}"

                # Choose item weight either from discrete options or from a range
                if 'weight_options' in sub_props:
                    item_weight = np.random.choice(sub_props['weight_options'])
                else:
                    item_weight = np.random.randint(sub_props['weight_range'][0], sub_props['weight_range'][1])

                # Random item price within the configured range
                item_price = round(np.random.uniform(sub_props['price_range'][0], sub_props['price_range'][1]), 2)

                # Compute standard batch size for this product
                standard_batch_size = compute_standard_batch_size(category, subcategory, item_weight)

                catalog_data.append({
                    'shelf_id': shelf_id,
                    'aisle': aisle,
                    'item_category': category,
                    'item_subcategory': subcategory,
                    'item_weight': item_weight,
                    'item_price': item_price,
                    'standard_batch_size': standard_batch_size
                })
    return pd.DataFrame(catalog_data)


def generate_inventory_df(product_catalog, product_hierarchy, inventory_type, store_max_stock_data=None):
    """
    Generate an inventory dataframe for either:
      - inventory_type = 'store'
      - inventory_type = 'warehouse'

    Rules:
      * Store:
          - maximum_stock taken from the store_max_stock_range of the product
            (not forced to be a multiple of the batch size)
          - current_stock = percentage of maximum_stock (more granular, can be partially consumed)
      * Warehouse:
          - maximum_stock is a multiple of standard_batch_size
          - capacity must fit at least 2 full batches
          - current_stock = SMALL number of FULL batches (e.g. 1–3) → fewer batches
    """
    data = []

    for _, product_row in product_catalog.iterrows():
        shelf_id = product_row['shelf_id']
        aisle = product_row['aisle']
        category = product_row['item_category']
        subcategory = product_row['item_subcategory']
        item_weight = product_row['item_weight']
        item_price = product_row['item_price']
        standard_batch_size = product_row['standard_batch_size']

        cat_props = product_hierarchy[category]
        sub_props = cat_props['subcategories'][subcategory]
        is_perishable = 'current_stock_probabilities' in sub_props

        # Minimum number of full batches we want the warehouse to be able to hold
        min_batches_capacity = 2

        # -------------------------------------------------
        # Maximum stock
        #   - STORE: random within configured store_max_stock_range
        #   - WAREHOUSE: multiple of standard_batch_size, at least 2 full batches
        # -------------------------------------------------
        if inventory_type == 'warehouse':
            # Case 1: warehouse_max_stock should be similar to store's, but aligned to batch size
            if 'warehouse_max_stock_equal_store' in sub_props and sub_props['warehouse_max_stock_equal_store']:
                store_max = store_max_stock_data.loc[
                    store_max_stock_data['shelf_id'] == shelf_id, 'maximum_stock'
                ].iloc[0]
                base_max = max(store_max, min_batches_capacity * standard_batch_size)
                # Round up to the next multiple of standard_batch_size
                maximum_stock = int(
                    ((base_max + standard_batch_size - 1) // standard_batch_size) * standard_batch_size
                )

            # Case 2: specific warehouse_max_stock_range
            elif 'warehouse_max_stock_range' in sub_props:
                base_max = np.random.randint(
                    sub_props['warehouse_max_stock_range'][0],
                    sub_props['warehouse_max_stock_range'][1]
                )
                base_max = max(base_max, min_batches_capacity * standard_batch_size)
                # Round down to a multiple of standard_batch_size, ensure at least 2 batches
                maximum_stock = (base_max // standard_batch_size) * standard_batch_size
                if maximum_stock < min_batches_capacity * standard_batch_size:
                    maximum_stock = min_batches_capacity * standard_batch_size

            # Case 3: fallback – warehouse capacity is a fraction of store capacity
            else:
                store_max = store_max_stock_data.loc[
                    store_max_stock_data['shelf_id'] == shelf_id, 'maximum_stock'
                ].iloc[0]
                base_max = int(np.random.uniform(0.1, 0.4) * store_max)
                base_max = max(base_max, min_batches_capacity * standard_batch_size)
                maximum_stock = (base_max // standard_batch_size) * standard_batch_size
                if maximum_stock < min_batches_capacity * standard_batch_size:
                    maximum_stock = min_batches_capacity * standard_batch_size

        else:  # STORE
            base_max = np.random.randint(
                sub_props['store_max_stock_range'][0],
                sub_props['store_max_stock_range'][1]
            )
            # Ensure at least enough capacity for 1 full batch
            if base_max < standard_batch_size:
                base_max = standard_batch_size
            maximum_stock = base_max

        # -------------------------------------------------
        # Current stock
        #   - Warehouse:
        #       * Perishables → 0 (not stored in central warehouse)
        #       * Non-perishables → small number of FULL batches (1–3)
        #   - Store:
        #       * Perishables → use sub_props['current_stock_probabilities']
        #       * Non-perishables → percentage of maximum_stock
        # -------------------------------------------------
        if inventory_type == 'warehouse':
            if is_perishable:
                # Perishable items are not stored in the warehouse
                current_stock = 0
            else:
                # Compute how many full batches the warehouse could hold
                max_full_batches_possible = maximum_stock // standard_batch_size

                if max_full_batches_possible == 0:
                    current_stock = 0
                else:
                    # Limit the number of batches actually present in the warehouse
                    max_full_batches_in_warehouse = 3  # <-- tune this to 2, 3, 5, ... as you prefer
                    n_full_batches = np.random.randint(
                        1, min(max_full_batches_in_warehouse, max_full_batches_possible) + 1
                    )
                    # Warehouse keeps only FULL batches (no partially consumed batches here)
                    current_stock = n_full_batches * standard_batch_size

        else:  # STORE
            if is_perishable:
                # Use the product-specific probability model (e.g. fruits & vegetables)
                raw_current = np.random.choice(sub_props['current_stock_probabilities'])
                current_stock = min(raw_current, maximum_stock)
            else:
                # Non-perishables in store: percentage of maximum_stock
                current_stock_ratio = np.random.uniform(
                    sub_props['current_stock_percent'][0],
                    sub_props['current_stock_percent'][1]
                )
                current_stock = int(maximum_stock * current_stock_ratio)
                if current_stock > maximum_stock:
                    current_stock = maximum_stock
                if current_stock == 0:
                    # Ensure at least some stock if capacity exists
                    current_stock = min(standard_batch_size, maximum_stock)

        shelf_weight = item_weight * maximum_stock
        item_visibility = round(np.random.uniform(0.05, 0.2), 6) if inventory_type == 'store' else None

        row = {
            'shelf_id': shelf_id,
            'aisle': aisle,
            'item_weight': item_weight,
            'shelf_weight': shelf_weight,
            'item_category': category,
            'item_subcategory': subcategory,
            'standard_batch_size': standard_batch_size,  # kept in the DF for batch generation
            'maximum_stock': maximum_stock,
            'current_stock': current_stock,
            'item_price': item_price
        }
        if inventory_type == 'store':
            row['item_visibility'] = item_visibility

        data.append(row)

    df = pd.DataFrame(data)

    if inventory_type == 'store':
        cols = [
            'shelf_id', 'aisle', 'item_weight', 'shelf_weight',
            'item_category', 'item_subcategory', 'item_visibility',
            # NOTE: standard_batch_size is kept in the dataframe
            # but will be dropped before saving the store parquet file
            'standard_batch_size',
            'maximum_stock', 'current_stock',
            'item_price'
        ]
    else:
        cols = [
            'shelf_id', 'aisle', 'item_weight', 'shelf_weight',
            'item_category', 'item_subcategory',
            'standard_batch_size',
            'maximum_stock', 'current_stock',
            'item_price'
        ]

    return df[cols]



# -------------------------------------------------------------------
# Batches generation (store / warehouse)
# -------------------------------------------------------------------
def generate_batches_data(inventory_df, product_hierarchy, location_type):
    """
    Generate batches for store or warehouse.

    For each (shelf_id, location_type):
      - standard_batch_size is taken from the inventory dataframe
      - each batch_quantity_total <= standard_batch_size
      - the sum of batch_quantity_total equals current_stock
    """
    batches_data = []

    for _, row in inventory_df.iterrows():
        shelf_id = row['shelf_id']
        current_stock = row['current_stock']
        category = row['item_category']
        subcategory = row['item_subcategory']
        standard_batch_size = row['standard_batch_size']

        # Skip if there is no stock
        if current_stock == 0:
            continue

        # Number of full batches + one possibly partially consumed batch
        full_batches = current_stock // standard_batch_size
        remainder = current_stock % standard_batch_size

        num_batches = full_batches + (1 if remainder > 0 else 0)

        # Determine if the item is perishable (to set different expiry dates)
        is_perishable = 'current_stock_probabilities' in product_hierarchy[category]['subcategories'][subcategory]

        for batch_index in range(num_batches):
            if batch_index < full_batches:
                batch_qty_total = standard_batch_size
            else:
                batch_qty_total = remainder

            if batch_qty_total <= 0:
                continue
            if batch_qty_total > standard_batch_size:
                # Safety check: never allow a batch bigger than the standard size
                batch_qty_total = standard_batch_size

            if location_type == 'in-store':
                batch_qty_store = batch_qty_total
                batch_qty_warehouse = 0
            else:  # warehouse
                batch_qty_store = 0
                batch_qty_warehouse = batch_qty_total

            # Expiry dates:
            #   - for perishables: closer expiry, older batches expire earlier
            #   - for non-perishables: long shelf life, older batches expire earlier
            if is_perishable:
                min_days = 2 + batch_index
                max_days = 15
                expiry_delta = timedelta(days=np.random.randint(min_days, max_days))
            else:
                min_days = 365 + 100 * batch_index
                max_days = min_days + 365
                expiry_delta = timedelta(days=np.random.randint(min_days, max_days))

            # Received date: batches with lower index are older (received earlier)
            days_ago = np.random.randint(0, 3) + max(0, (num_batches - 1 - batch_index))
            received_date = datetime.now() - timedelta(days=days_ago)

            batches_data.append({
                'shelf_id': shelf_id,
                'batch_code': f'B-{np.random.randint(1000, 9999)}',
                'item_category': category,
                'item_subcategory': subcategory,
                'standard_batch_size': standard_batch_size,
                'received_date': received_date.strftime('%Y-%m-%d'),
                'expiry_date': (datetime.now() + expiry_delta).strftime('%Y-%m-%d'),
                'batch_quantity_total': batch_qty_total,
                'batch_quantity_store': batch_qty_store,
                'batch_quantity_warehouse': batch_qty_warehouse,
                'location': location_type
            })

    return pd.DataFrame(batches_data)


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
print("Generating product catalog...")
product_catalog = generate_product_catalog(product_hierarchy)



# Saving db in parquet
print("Generating store inventory...")
store_inventory_df = generate_inventory_df(product_catalog, product_hierarchy, 'store')
store_inventory_to_save = store_inventory_df.drop(columns=['standard_batch_size'])
store_inventory_to_save.to_parquet('data/store_inventory_final.parquet', index=False)
print("Store inventory saved successfully!")

print("Generating warehouse inventory...")
warehouse_inventory_df = generate_inventory_df(
    product_catalog, product_hierarchy, 'warehouse', store_max_stock_data=store_inventory_df
)
warehouse_inventory_to_save = warehouse_inventory_df.drop(columns=['standard_batch_size'])
warehouse_inventory_to_save.to_parquet('data/warehouse_inventory_final.parquet', index=False)
print("Warehouse inventory saved successfully!")

print("Generating batches dataset for the store...")
store_batches_df = generate_batches_data(store_inventory_df, product_hierarchy, 'in-store')
store_batches_df.to_parquet('data/store_batches.parquet', index=False)
print("Batches dataset for the store saved successfully!")

print("Generating batches dataset for the warehouse...")
warehouse_batches_df = generate_batches_data(warehouse_inventory_df, product_hierarchy, 'warehouse')
warehouse_batches_df.to_parquet('data/warehouse_batches.parquet', index=False)
print("Batches dataset for the warehouse saved successfully!")

print("\nAll datasets generated and saved successfully!")

# Saving db in csv
store_inventory_to_save.to_csv('data/db_csv/store_inventory_final.csv', index=False)
print("Store inventory csv saved successfully!")

warehouse_inventory_to_save.to_csv('data/db_csv/warehouse_inventory_final.csv', index=False)
print("Warehouse inventory csv saved successfully!")

store_batches_df.to_csv('data/db_csv/store_batches.csv', index=False)
print("Batches dataset csv for the store saved successfully!")

warehouse_batches_df.to_csv('data/db_csv/warehouse_batches.csv', index=False)
print("Batches dataset csv for the warehouse saved successfully!")



print("\nAll datasets generated and saved successfully!")