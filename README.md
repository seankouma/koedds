# Koedds
A national real estate investment report and visualization analyzing Redfin data with Spark. The goal of this project was to combine historical ROI data with a scoring model the CDC uses to assess county vulnerability to ultimately guide decision making around ideal locations for investment.

## Data
This analysis pulls data from several different sources.
 - Redfin real estate data which contains a wide variety of data points for each county, each month going back to 2012.
 - Social Vulnerability Index data from the CDC which combines ~17 different factors (primarily socioeconomic) to assess counties a risk score in the event of a economic or physical crisis.
 - Data from Fysallida, a previous CSU CS 455 project which assigned crash and resiliency scores for counties across the US. While we didn't use this data in our predictions or for our decision making, it contains helpful information and was useful in helping us put our results inside something that can be visualized using the Monocle service on Urban-Sustain.

## Files
Inside the Spark folder you will find the code we ran on our Spark cluster to parse the Redfin data (stored on HDFS) into something to actually provide us return on investment scores based off of growth performance from the past 12 years.

Most of the code relating to the actual analysis and visualization of the data is inside the analysis/analyze.ipynb Jupyter notebook.
