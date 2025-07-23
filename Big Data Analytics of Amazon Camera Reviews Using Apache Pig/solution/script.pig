inputFile = LOAD '/home/ca7kr/Downloads/Assignment5PIG/input/data' Using PigStorage('\t') AS (marketplace: chararray, customer_id: int, review_id: chararray, product_id: chararray,	product_parent: int, product_title: chararray, product_category: chararray, star_rating: int, helpful_votes: int, total_votes: int, vine: chararray, verified_purchase: chararray, review_headline: chararray, review_body: chararray, review_date: Datetime);

ranked = rank inputFile;
noHeader = filter ranked by (rank_inputFile > 1);

inputUpdateFile = foreach noHeader generate marketplace, customer_id,review_id,product_id, product_parent, product_title, product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body,review_date;

MarketpProductCGroup = Group inputUpdateFile By (marketplace, product_category);
marketplaceProductCategoryGroup = FOREACH MarketpProductCGroup Generate FLATTEN(group) as (marketplace, product_category), COUNT(inputUpdateFile.review_id) as totalReviewID, AVG(inputUpdateFile.star_rating) as averageStarRatings;
Dump marketplaceProductCategoryGroup;
STORE marketplaceProductCategoryGroup INTO '/home/ca7kr/Downloads/Assignment5PIG/output1.2';


FilteredRecords = FILTER inputUpdateFile BY (UPPER(marketplace) != 'US');
Dump FilteredRecords;
A = Limit FilteredRecords 5;
Dump A;

MarketpProductCFiltGroup = Group FilteredRecords By (marketplace, product_category);
marketplaceProductCategoryFiltGroup = Foreach MarketpProductCFiltGroup Generate FLATTEN(group) As (marketplace, product_category), COUNT(FilteredRecords.review_id) as totalReviewID, AVG(FilteredRecords.star_rating) As averageStarRatings;
Dump marketplaceProductCategoryFiltGroup;
STORE marketplaceProductCategoryFiltGroup INTO '/home/ca7kr/Downloads/Assignment5PIG/output1.2.1';






reviewDateGroup = Group inputUpdateFile By (review_date);
reviewDate_totalNumberReviewID= Foreach reviewDateGroup Generate FLATTEN(group) As (review_date), COUNT(inputUpdateFile.review_id) as totalReviewID;
Dump reviewDate_totalNumberReviewID;
STORE reviewDate_totalNumberReviewID INTO '/home/ca7kr/Downloads/Assignment5PIG/output2.1';

productIDGroup = Group inputUpdateFile By (product_id);
forEachProductIDAvgHelpfulvotesAvgTotalvotes= Foreach productIDGroup Generate FLATTEN(group) As (product_id), AVG(inputUpdateFile.helpful_votes) as averageHelpfulVotes, AVG(inputUpdateFile.total_votes) as averageTotalVotes ;
Dump forEachProductIDAvgHelpfulvotesAvgTotalvotes;
STORE forEachProductIDAvgHelpfulvotesAvgTotalvotes INTO '/home/ca7kr/Downloads/Assignment5PIG/output2.2';

LimitProductIDAvgHelpfulvotesAvgTotalvotes =  Limit forEachProductIDAvgHelpfulvotesAvgTotalvotes 10;
Dump LimitProductIDAvgHelpfulvotesAvgTotalvotes;
STORE LimitProductIDAvgHelpfulvotesAvgTotalvotes INTO '/home/ca7kr/Downloads/Assignment5PIG/output2.2.1.a';

OrderedProductIDAvgHelpfulvotesAvgTotalvotes = Order forEachProductIDAvgHelpfulvotesAvgTotalvotes By averageTotalVotes Desc;
LimitOrderedProductIDAvgHelpfulvotesAvgTotalvotes = Limit OrderedProductIDAvgHelpfulvotesAvgTotalvotes 10;
Dump LimitOrderedProductIDAvgHelpfulvotesAvgTotalvotes;
STORE LimitOrderedProductIDAvgHelpfulvotesAvgTotalvotes INTO '/home/ca7kr/Downloads/Assignment5PIG/output2.2.1.b';




FilteredVerifiedPurchases = FILTER inputUpdateFile BY (UPPER(verified_purchase) == 'Y');
productCategoryGroup = Group FilteredVerifiedPurchases By (product_category);
forEachProductCategoryTotalNumberOfProductID = Foreach productCategoryGroup Generate FLATTEN(group) As (product_category), COUNT(FilteredVerifiedPurchases.product_id) as totalNumberOfProductID;
Dump forEachProductCategoryTotalNumberOfProductID;
STORE forEachProductCategoryTotalNumberOfProductID INTO '/home/ca7kr/Downloads/Assignment5PIG/output3.1.1';

starRatingGroup = Group FilteredVerifiedPurchases By (star_rating);
forEachStarRatingSumOfHelpfulvotesSumOfTotalvotesAvgvotes = Foreach starRatingGroup Generate FLATTEN(group) As (star_rating), SUM(FilteredVerifiedPurchases.helpful_votes) as sumOfHelpfulVotes, SUM(FilteredVerifiedPurchases.total_votes) as sumOfTotalVotes, (SUM(FilteredVerifiedPurchases.helpful_votes)/(DOUBLE)SUM(FilteredVerifiedPurchases.total_votes)) as avg_votes;
Dump forEachStarRatingSumOfHelpfulvotesSumOfTotalvotesAvgvotes;
STORE forEachStarRatingSumOfHelpfulvotesSumOfTotalvotesAvgvotes INTO '/home/ca7kr/Downloads/Assignment5PIG/output3.1.2';

orderedForEachStarRatingSumOfHelpfulvotesSumOfTotalvotesAvgvotesDesc = Order forEachStarRatingSumOfHelpfulvotesSumOfTotalvotesAvgvotes By avg_votes Desc;
Dump orderedForEachStarRatingSumOfHelpfulvotesSumOfTotalvotesAvgvotesDesc;
STORE forEachStarRatingSumOfHelpfulvotesSumOfTotalvotesAvgvotes INTO '/home/ca7kr/Downloads/Assignment5PIG/output3.1.2.1.a';

orderedForEachStarRatingSumOfHelpfulvotesSumOfTotalvotesAvgvotesAscen = Order forEachStarRatingSumOfHelpfulvotesSumOfTotalvotesAvgvotes By avg_votes ASC;
Dump orderedForEachStarRatingSumOfHelpfulvotesSumOfTotalvotesAvgvotesAscen;
STORE forEachStarRatingSumOfHelpfulvotesSumOfTotalvotesAvgvotes INTO '/home/ca7kr/Downloads/Assignment5PIG/output3.1.2.1.b';

