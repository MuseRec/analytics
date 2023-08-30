import numpy as np
import seaborn as sns

SAVE_PLOTS = False

np.random.seed(42)
sns.set_palette('colorblind')

MODEL_TYPES = [
    'Recommendations',
    'Random',
    'Overall'
]

COLOUR_MAPPING = {
    model: colour
    for model, colour in zip(MODEL_TYPES, sns.color_palette()[:len(MODEL_TYPES)])
}