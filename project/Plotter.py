import pandas
from ggplot import *


class Plotter:
    # This function creates a data frame from a set of names and lists.
    def __lists_to_dataframes(self, names, lists):
        for listIt in range(len(lists)-1):
            list1 = lists[listIt]
            list2 = lists[listIt+1]
            if (len(list1) != len(list2)):
                print("The lists cannot be converted to a data frame because they differ in size.")
                exit()
        lists_dictionary = {}
        for (name,list) in zip(names,lists):
            lists_dictionary.update({name:list})
        dataframe = pandas.DataFrame.from_dict(lists_dictionary)

        return dataframe


    # The show_plot method creates a 2d graph of points.
    # Parameters:
    #   self - The first parameter in every method. Refers to this object.
    #   title - A string that will show on the graph.
    #   axis_labels - A list with two string elements; the first being the label for the x-axis and
    #       the second being the label for the y-axis.
    #   data_lists - A list holding two other lists as elements. The first list are the x positions
    #       of all the points, and the second list are the y positions.
    #   slope - A real value that represents the slope of the best fit line.
    #   intercept - A real value that represents the intercept of the best fit line.
    #   r2 - A real value that contains the R-squared value.
    def show_plot(self, title, axis_labels, data_lists, slope=None, intercept=None, r2=None):
        if (r2 is None):
            x_axis_label = axis_labels[0]
        else:
            # The following is a workaround because it's difficult to create labels in Python's version of ggplot.
            x_axis_label = 'y = {0:.4f}x + {1:.4f}                             {2}                                            R-squared: {3:.6f}'\
                .format(slope, intercept, axis_labels[0], r2)

        # Still working through the workaround, we replace the x-axis with the newly formatted x-axis
        # we just created.
        new_axis_labels = axis_labels[1:]
        new_axis_labels.insert(0,x_axis_label)

        # ggplot requires a data frame.
        dataframe = self.__lists_to_dataframes(new_axis_labels, data_lists)

        # Just for fun build up the plot in two steps.
        plot = ggplot(dataframe, aes(x = new_axis_labels[0], y = new_axis_labels[1])) +\
            geom_point(color = "black") + theme_bw() +\
            ggtitle(title)

        # Step 2.
        if ((intercept != None) and (slope != None)):
            plot = plot + geom_abline(intercept = intercept, slope = slope)

        plot.show()
