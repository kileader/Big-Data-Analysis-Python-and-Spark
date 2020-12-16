from InputCsvs import TrackStarLinearRegression
from Plotter import Plotter
from Algorithms import Algorithms

def main():
    fileName = 'D:\\projects\\unit1\\project\\track_star_data.three_columns.csv'
    trackClass = TrackStarLinearRegression(fileName)
    data = trackClass.read_Csv()

    speeds, ages, initials = data

    plotterClass = Plotter()

    algorithmsClass = Algorithms()
    speed_by_age_best_fit = algorithmsClass.find_best_fit(ages, speeds)
    speed_by_initials_best_fit = algorithmsClass.find_best_fit(initials, speeds)

    sba_bf_slope, sba_bf_intercept, sba_bf_R_squared = speed_by_age_best_fit
    sbi_bf_slope, sbi_bf_intercept, sbi_bf_R_squared = speed_by_initials_best_fit

    plotterClass.show_plot("Speed by Age", ["Age", "Speed"], [ages, speeds], sba_bf_slope, sba_bf_intercept, sba_bf_R_squared)
    plotterClass.show_plot("Speed by Initials", ["Initials Order Number", "Speed"], [initials, speeds], sbi_bf_slope, sbi_bf_intercept, sbi_bf_R_squared)


if __name__ == "__main__":
    main()