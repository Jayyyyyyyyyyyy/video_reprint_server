

from colormath.color_objects import XYZColor, sRGBColor,LabColor
from colormath.color_conversions import convert_color
from colormath.color_objects import LabColor
from colormath.color_diff import delta_e_cie1976, delta_e_cie1994


def ColourDistance(r1, g1, b1, r2, g2, b2):
    rgb1 = [r1, g1, b1]
    rgb2 = [r2, g2, b2]
    def rgb2lab(rgb):
        rgb = sRGBColor(rgb_r=rgb[0], rgb_g=rgb[1], rgb_b=rgb[2], is_upscaled=True)
        xyz = convert_color(rgb, XYZColor)
        lab = convert_color(xyz,LabColor)
        return lab
    lab1 = rgb2lab(rgb1)
    lab2 = rgb2lab(rgb2)
    delta_e2 = delta_e_cie1994(lab1, lab2)
    return delta_e2




# ColourDistance(251,253,138,239,223,105)