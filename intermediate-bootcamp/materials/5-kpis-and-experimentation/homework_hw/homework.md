# Google Chrome's API and Experimentation

## User Journey

I started to use Google Chrome as the main search engine on my laptop when I become a Macbook user nine years ago. I prefer to use Chrome then the default Safari browser on Macbook for multiple reasons. First, it allows me to google easily as long as I open a new window. Second, I prefer the UI design of Chrome. Third, it presents searching results in a accurate ranking order. And most importantly, it incorporates a new AI functionality now. Another thing to notice is that Chrome browser offers image searching functionality, which is convenient and powerful when I'm curious about plants and flowers when taking a walk in the neighborhood.

## Experiments

### Experiment 1: Ads / monetization
Objective: \
we want to test when users search for products or other merchandise information on Chrome, whether presenting the searched product as well as related recommended product links first before other links will increase the ad CTR.

Leading matric: number of users who search products on Chrome 

Lagging metric: number of users who click into the links of recommended products on the searching page.

Null Hypothesis: the recommended product links ranked first will not increase the chance of ad CTR.\
Alternative Hypothesis: the recommended product links ranked first will increase the chance of ad CTR.

Test cell allocation: 50%-50%

### Experiment 2: Incremental signup lift from search
Objective: \
we want to add a reminder band on the webpage of users who search on Chrome but don't sign up, to see if that can encourage them to sign up google email.

Leading matric: number of users who search on Chrome / number of Chrome eligible users

Lagging metric: number of users who signup after searching on Chrome / number of Chrome eligible users

Null Hypothesis: the reminder band will not increase the chance of new users to sign up.\
Alternative Hypothesis: the reminder band will increase the chance of new users to sign up.

Test cell allocation: 50%-50%

### Experiment 3: AI generated result's successful search rate increase
Objective: \
We want to see if presenting AI generated results on the page will lead to users' ideal searching results.

Leading matric: average of users' successful search rate
- Good click: user clicks a result and has dwell time ≥ X seconds

Lagging metric: average of users' downstream conversion
- Downstream conversion: purchase/signup/save/watch/message per search session

Null Hypothesis: presenting a AI generated result on the searching page will not increase the successful search rate.\
Alternative Hypothesis: presenting a AI generated result on the searching page will increase the successful search rate.

Test cell allocation: 50%-50%