import math

class Algorithms:
    def find_best_fit(self, x_values, y_values):
        
        # Average of x values
        x_sum = 0
        for xi in x_values:
            x_sum += xi
        x_bar = x_sum / len(x_values)

        # Average of y values
        y_sum = 0
        for yi in y_values:
            y_sum += yi
        y_bar = y_sum / len(y_values)

        # y values minus y average
        y_bar_diffs = []
        for yi in y_values:
            y_bar_diffs.append(yi - y_bar)

        # x values minus x average
        x_bar_diffs = []
        for xi in x_values:
            x_bar_diffs.append(xi - x_bar)
        
        # Numerator of slope
        numerator = 0
        for (x_bar_diff, y_bar_diff) in zip(x_bar_diffs, y_bar_diffs):
            numerator += x_bar_diff * y_bar_diff
        
        # Denominator of slope
        denominator = 0
        for x_bar_diff in x_bar_diffs:
            denominator += x_bar_diff ** 2

        # Best fit line slope
        m_hat = numerator / denominator

        # Best fit line y intercept
        b_hat = y_bar - m_hat * x_bar

        # y points for best fit line
        y_hat_list = []
        for xi in x_values:
            y_hat_list.append(m_hat * xi + b_hat)
        
        # R squared
        R2_numerator, R2_denominator = 0, 0
        for (yi, yi_hat, y_bar_diff) in zip(y_values, y_hat_list, y_bar_diffs):
            R2_numerator += (yi - yi_hat) ** 2
            R2_denominator += y_bar_diff ** 2
        R_squared = 1 - R2_numerator / R2_denominator

        return (m_hat, b_hat, R_squared)