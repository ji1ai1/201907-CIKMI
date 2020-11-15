library(data.table)

Items = fread("/tcdata/item.csv", col.names = c("ItemId", "CategoryId", "ShopId", "BrandId"))
Users = fread("/tcdata/user.csv", col.names = c("UserId", "Gender", "Age", "PurchasingPower"))
Behaviours = fread("/tcdata/user_behavior.csv", col.names = c("UserId", "ItemId", "BehaviourType", "Timestamp"))
Behaviours = merge(Behaviours, Items, by = "ItemId")
print(nrow(Behaviours))


UC = Behaviours[, .(UCScore = .N), .(UserId, CategoryId)]
US = Behaviours[, .(USScore = .N), .(UserId, ShopId)]
UB = Behaviours[, .(UBScore = .N), .(UserId, BrandId)]




Behaviours = Behaviours[order(Timestamp), .(ItemId, BehaviourType, Timestamp, CategoryId, ShopId, BrandId, Index = .N:1, UserScore = .N), .(UserId)]

ItemData = Behaviours[, .(ItemNoNRecords = .N), .(ItemId)]
ItemData = ItemData[order(-ItemNoNRecords)]




UserCategoryRelatedItems = Behaviours[ItemId %in% ItemData[ItemNoNRecords >= 1]$ItemId]
UserCategoryRelatedItems = merge(UserCategoryRelatedItems[, .(UserId, UserScore, ItemId, CategoryId, Index)]
	, UserCategoryRelatedItems[, .(UserId, RelatedItemId = ItemId, CategoryId, RelatedIndex = Index)]
, by = c("UserId", "CategoryId"), allow.cartesian = T)
UserCategoryRelatedItems = UserCategoryRelatedItems[ItemId != RelatedItemId]
UserCategoryRelatedItems = merge(UserCategoryRelatedItems, UC, by = c("UserId", "CategoryId"))
UserCategoryRelatedItems[, UiriNoIndexDifference := abs(Index - RelatedIndex)]
UserCategoryRelatedItems = UserCategoryRelatedItems[, .(UiriNoIndexDifference = sum(1 / UiriNoIndexDifference ** 4) ** 0.25), .(UserId, UCScore, ItemId, RelatedItemId)]

CategoryRelatedItems = UserCategoryRelatedItems
CategoryRelatedItems[, RelatingScore := (1 + UiriNoIndexDifference ** 4) / UCScore ** 0.2]
CategoryRelatedItems = CategoryRelatedItems[, .(RelatingScore = sum(RelatingScore)), .(ItemId, RelatedItemId)]
CategoryRelatedItems = rbind(CategoryRelatedItems, fread("te", col.names = c("ItemId", "RelatedItemId", "RelatingScore"))[ItemId %in% Items$ItemId & RelatedItemId %in% Items$ItemId])
CategoryRelatedItems = CategoryRelatedItems[, .(RelatingScore = sum(RelatingScore)), .(ItemId, RelatedItemId)]
CategoryRelatedItems = merge(CategoryRelatedItems, ItemData, by = "ItemId")
CategoryRelatedItems = CategoryRelatedItems[, .(ItemId, RelatedItemId, RelatingScore = RelatingScore / ItemNoNRecords)]








UserShopRelatedItems = Behaviours[ItemId %in% ItemData[ItemNoNRecords >= 1]$ItemId]
UserShopRelatedItems = merge(UserShopRelatedItems[, .(UserId, UserScore, ItemId, ShopId, Index)]
	, UserShopRelatedItems[, .(UserId, RelatedItemId = ItemId, ShopId, RelatedIndex = Index)]
, by = c("UserId", "ShopId"), allow.cartesian = T)
UserShopRelatedItems = merge(UserShopRelatedItems, US, by = c("UserId", "ShopId"))
UserShopRelatedItems[, UiriNoIndexDifference := abs(Index - RelatedIndex)]
UserShopRelatedItems = UserShopRelatedItems[, .(UiriNoIndexDifference = sum(1 / UiriNoIndexDifference ** 4) ** 0.25), .(UserId, USScore, ItemId, RelatedItemId)]

ShopRelatedItems = UserShopRelatedItems
ShopRelatedItems[, RelatingScore := (1 + UiriNoIndexDifference ** 4) / USScore ** 0.2]
ShopRelatedItems = ShopRelatedItems[, .(RelatingScore = sum(RelatingScore)), .(ItemId, RelatedItemId)]
ShopRelatedItems = rbind(ShopRelatedItems, fread("te2", col.names = c("ItemId", "RelatedItemId", "RelatingScore"))[ItemId %in% Items$ItemId & RelatedItemId %in% Items$ItemId])
ShopRelatedItems = ShopRelatedItems[, .(RelatingScore = sum(RelatingScore)), .(ItemId, RelatedItemId)]
ShopRelatedItems = merge(ShopRelatedItems, ItemData, by = "ItemId")
ShopRelatedItems = ShopRelatedItems[, .(ItemId, RelatedItemId, RelatingScore = RelatingScore / ItemNoNRecords)]








UserBrandRelatedItems = Behaviours[ItemId %in% ItemData[ItemNoNRecords >= 1]$ItemId & BrandId != -1]
UserBrandRelatedItems = merge(UserBrandRelatedItems[, .(UserId, UserScore, ItemId, BrandId, Timestamp, Index)]
	, UserBrandRelatedItems[, .(UserId, RelatedItemId = ItemId, BrandId, RelatedTimestamp = Timestamp, RelatedIndex = Index)]
, by = c("UserId", "BrandId"), allow.cartesian = T)
UserBrandRelatedItems = UserBrandRelatedItems[ItemId != RelatedItemId]
UserBrandRelatedItems = merge(UserBrandRelatedItems, UB, by = c("UserId", "BrandId"))
UserBrandRelatedItems[, UiriNoIndexDifference := abs(Index - RelatedIndex)]
UserBrandRelatedItems = UserBrandRelatedItems[, .(UiriNoIndexDifference = sum(1 / UiriNoIndexDifference ** 4) ** 0.25), .(UserId, UBScore, ItemId, RelatedItemId)]

BrandRelatedItems = UserBrandRelatedItems
BrandRelatedItems[, RelatingScore := (1 + UiriNoIndexDifference ** 4) / UBScore ** 0.2]
BrandRelatedItems = BrandRelatedItems[, .(RelatingScore = sum(RelatingScore)), .(ItemId, RelatedItemId)]
BrandRelatedItems = rbind(BrandRelatedItems, fread("te3", col.names = c("ItemId", "RelatedItemId", "RelatingScore"))[ItemId %in% Items$ItemId & RelatedItemId %in% Items$ItemId])
BrandRelatedItems = BrandRelatedItems[, .(RelatingScore = sum(RelatingScore)), .(ItemId, RelatedItemId)]
BrandRelatedItems = merge(BrandRelatedItems, ItemData, by = "ItemId")
BrandRelatedItems = BrandRelatedItems[, .(ItemId, RelatedItemId, RelatingScore = RelatingScore / ItemNoNRecords)]






NRelatedItems = fread("te4", col.names = c("ItemId", "RelatedItemId", "RelatingScore"))[ItemId %in% Items$ItemId & RelatedItemId %in% Items$ItemId]
NRelatedItems = merge(NRelatedItems, ItemData, by = "ItemId")
NRelatedItems = NRelatedItems[, .(ItemId, RelatedItemId, RelatingScore = RelatingScore / ItemNoNRecords)]







RelatedItems = CategoryRelatedItems[, .(ItemId, RelatedItemId, CategoryRelatingScore = RelatingScore)]
RelatedItems = merge(RelatedItems, ShopRelatedItems[, .(ItemId, RelatedItemId, ShopRelatingScore = RelatingScore)], by = c("ItemId", "RelatedItemId"), all = T)
RelatedItems = merge(RelatedItems, BrandRelatedItems[, .(ItemId, RelatedItemId, BrandRelatingScore = RelatingScore)], by = c("ItemId", "RelatedItemId"), all = T)
RelatedItems = merge(RelatedItems, NRelatedItems[, .(ItemId, RelatedItemId, NRelatingScore = RelatingScore)], by = c("ItemId", "RelatedItemId"), all = T)
RelatedItems[is.na(RelatedItems)] = 0
RelatedItems = RelatedItems[, .(ItemId, RelatedItemId, RelatingScore = CategoryRelatingScore + ShopRelatingScore + BrandRelatingScore + NRelatingScore)]
RelatedItems = merge(RelatedItems, Items, by.x = "RelatedItemId", by.y = "ItemId")
rm(CategoryRelatedItems)
rm(ShopRelatedItems)
rm(BrandRelatedItems)
rm(NRelatedItems)
print(nrow(RelatedItems))


TestingTimestamp = (86399 + max(Behaviours$Timestamp)) %/% 86400 * 86400
print(TestingTimestamp)

NBatches = 12
Prediction = NULL
for (a in 1:NBatches)
{
	print(paste0(a, " ", date()))
	UserPredictedItems = Behaviours[UserId %in% Users$UserId & UserId %% NBatches == a - 1]
	UserPredictedItems$TypeScore = 1
	UserPredictedItems$TypeScore[UserPredictedItems$BehaviourType == "cart"] = 5
	UserPredictedItems$TypeScore[UserPredictedItems$BehaviourType == "fav"] = 5
	UserPredictedItems$TypeScore[UserPredictedItems$BehaviourType == "buy"] = 1
	UserPredictedItems[, UserItemScore := (86400 / (TestingTimestamp - Timestamp)) ** 0.5 / Index ** 0.5 * TypeScore]
	UserPredictedItems = UserPredictedItems[, .(UserItemScore = sum(UserItemScore)), .(UserId, ItemId)]
	UserPredictedItems = merge(UserPredictedItems, RelatedItems, by = "ItemId", allow.cartesian = T)
	UserPredictedItems[, Score := UserItemScore ** 0.5 * RelatingScore ** 0.5]
	UserPredictedItems = UserPredictedItems[, .(Score = sum(Score)), .(UserId, ItemId = RelatedItemId, CategoryId)]
	UserPredictedItems = merge(UserPredictedItems, Behaviours[UserId %in% Users$UserId & UserId %% NBatches == a - 1, .(UserCategoryNoScore = length(BehaviourType[BehaviourType == "buy"])), .(UserId, CategoryId)], by = c("UserId", "CategoryId"), all.x = T)
	UserPredictedItems[is.na(UserPredictedItems)] = 0
	UserPredictedItems = UserPredictedItems[, .(UserId, ItemId, Score = Score * (1 + UserCategoryNoScore) ** -1)]
	PredictionA = UserPredictedItems
	PredictionA = merge(PredictionA, Behaviours[UserId %% NBatches == a - 1, .(A = 1), .(UserId, ItemId)], by = c("UserId", "ItemId"), all.x = T)
	PredictionA = PredictionA[is.na(A), .(UserId, ItemId, Score)]
	PredictionA = merge(PredictionA, ItemData, by = "ItemId")
	PredictionA = PredictionA[order(-Score), .(ItemId = ItemId[1:50], Score = Score[1:50]), .(UserId)]
    
	Prediction = rbind(Prediction, PredictionA)
}

FinalPrediction = merge(Users[, .(UserId)], Prediction, by = "UserId", all.x = T)
FinalPrediction = FinalPrediction[, .(ItemId = unique(c(ItemId[order(-Score)][!is.na(ItemId[order(-Score)])], ItemData$ItemId[1:50]))[1:50]), .(UserId)]



Submission = FinalPrediction[, .(PredictedString = paste(ItemId, collapse =",")), .(UserId)]
write.table(Submission, "result.csv", sep = ",", quote = F, row.names = F, col.names = F)
