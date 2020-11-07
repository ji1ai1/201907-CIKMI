library(data.table)

TrainingBehaviours = TrainingBehaviours[order(Timestamp), .(ItemId, BehaviourType, Timestamp, CategoryId, ShopId, BrandId, Index = .N:1, UserScore = .N), .(UserId)]
TrainingUC = TrainingBehaviours[, .(UCScore = .N), .(UserId, CategoryId)]
TrainingUS = TrainingBehaviours[, .(USScore = .N), .(UserId, ShopId)]
TrainingUB = TrainingBehaviours[, .(UBScore = .N), .(UserId, BrandId)]

TrainingItemData = TrainingBehaviours[, .(ItemNoNRecords = .N), .(ItemId)]
TrainingItemData = TrainingItemData[order(-ItemNoNRecords)]

TrainingUserCategoryRelatedItems = TrainingBehaviours[ItemId %in% TrainingItemData[ItemNoNRecords >= 1]$ItemId]
TrainingUserCategoryRelatedItems = merge(TrainingUserCategoryRelatedItems[, .(UserId, UserScore, ItemId, CategoryId, Timestamp, Index)]
	, TrainingUserCategoryRelatedItems[, .(UserId, RelatedItemId = ItemId, CategoryId, RelatedTimestamp = Timestamp, RelatedIndex = Index)]
, by = c("UserId", "CategoryId"), allow.cartesian = T)
TrainingUserCategoryRelatedItems = TrainingUserCategoryRelatedItems[ItemId != RelatedItemId]
TrainingUserCategoryRelatedItems = merge(TrainingUserCategoryRelatedItems, TrainingUC, by = c("UserId", "CategoryId"))
TrainingUserCategoryRelatedItems[, UiriNoIndexDifference := abs(Index - RelatedIndex)]
TrainingUserCategoryRelatedItems = TrainingUserCategoryRelatedItems[, .(UiriNoIndexDifference = sum(1 / UiriNoIndexDifference ** 4) ** 0.25), .(UserId, UCScore, ItemId, RelatedItemId)]




TrainingUserShopRelatedItems = TrainingBehaviours[ItemId %in% TrainingItemData[ItemNoNRecords >= 1]$ItemId]
TrainingUserShopRelatedItems = merge(TrainingUserShopRelatedItems[, .(UserId, UserScore, ItemId, ShopId, Timestamp, Index)]
	, TrainingUserShopRelatedItems[, .(UserId, RelatedItemId = ItemId, ShopId, RelatedTimestamp = Timestamp, RelatedIndex = Index)]
, by = c("UserId", "ShopId"), allow.cartesian = T)
TrainingUserShopRelatedItems = TrainingUserShopRelatedItems[ItemId != RelatedItemId]
TrainingUserShopRelatedItems = merge(TrainingUserShopRelatedItems, TrainingUS, by = c("UserId", "ShopId"))
TrainingUserShopRelatedItems[, UiriNoIndexDifference := abs(Index - RelatedIndex)]
TrainingUserShopRelatedItems = TrainingUserShopRelatedItems[, .(UiriNoIndexDifference = sum(1 / UiriNoIndexDifference ** 4) ** 0.25), .(UserId, USScore, ItemId, RelatedItemId)]




TrainingUserBrandRelatedItems = TrainingBehaviours[ItemId %in% TrainingItemData[ItemNoNRecords >= 1]$ItemId & BrandId != -1]
TrainingUserBrandRelatedItems = merge(TrainingUserBrandRelatedItems[, .(UserId, UserScore, ItemId, BrandId, Timestamp, Index)]
	, TrainingUserBrandRelatedItems[, .(UserId, RelatedItemId = ItemId, BrandId, RelatedTimestamp = Timestamp, RelatedIndex = Index)]
, by = c("UserId", "BrandId"), allow.cartesian = T)
TrainingUserBrandRelatedItems = TrainingUserBrandRelatedItems[ItemId != RelatedItemId]
TrainingUserBrandRelatedItems = merge(TrainingUserBrandRelatedItems, TrainingUB, by = c("UserId", "BrandId"))
TrainingUserBrandRelatedItems[, UiriNoIndexDifference := abs(Index - RelatedIndex)]
TrainingUserBrandRelatedItems = TrainingUserBrandRelatedItems[, .(UiriNoIndexDifference = sum(1 / UiriNoIndexDifference ** 4) ** 0.25), .(UserId, UBScore, ItemId, RelatedItemId)]



TrainingCategoryRelatedItems = TrainingUserCategoryRelatedItems
TrainingCategoryRelatedItems[, RelatingScore := (1 + UiriNoIndexDifference ** 4) / UCScore ** 0.2]
TrainingCategoryRelatedItems = TrainingUserCategoryRelatedItems[, .(RelatingScore = sum(RelatingScore)), .(ItemId, RelatedItemId)]
TrainingCategoryRelatedItems = TrainingCategoryRelatedItems[order(-RelatingScore)][1:(0.4 * nrow(TrainingCategoryRelatedItems))]
write.table(TrainingCategoryRelatedItems, "te", sep = ",", row.names = F, col.names = F, quote = F)

TrainingShopRelatedItems = TrainingUserShopRelatedItems
TrainingShopRelatedItems[, RelatingScore := (1 + UiriNoIndexDifference ** 4) / USScore ** 0.2]
TrainingShopRelatedItems = TrainingUserShopRelatedItems[, .(RelatingScore = sum(RelatingScore)), .(ItemId, RelatedItemId)]
TrainingShopRelatedItems = TrainingShopRelatedItems[order(-RelatingScore)][1:(0.4 * nrow(TrainingShopRelatedItems))]
write.table(TrainingShopRelatedItems, "te2", sep = ",", row.names = F, col.names = F, quote = F)

TrainingBrandRelatedItems = TrainingUserBrandRelatedItems
TrainingBrandRelatedItems[, RelatingScore := (1 + UiriNoIndexDifference ** 4) / UBScore ** 0.2]
TrainingBrandRelatedItems = TrainingUserBrandRelatedItems[, .(RelatingScore = sum(RelatingScore)), .(ItemId, RelatedItemId)]
TrainingBrandRelatedItems = TrainingBrandRelatedItems[order(-RelatingScore)][1:(0.4 * nrow(TrainingBrandRelatedItems))]
write.table(TrainingBrandRelatedItems, "te3", sep = ",", row.names = F, col.names = F, quote = F)





NBatches = 8
TrainingNRelatedItems = NULL
for (a in 1:NBatches)
{
	print(paste0(a, " ", date()))
	TrainingUserNRelatedItemsA = TrainingBehaviours[ItemId %in% TrainingItemData[ItemNoNRecords >= 32]$ItemId]
	TrainingUserNRelatedItemsA = merge(TrainingUserNRelatedItemsA[ItemId %% NBatches == a - 1, .(UserId, UserScore, ItemId, Timestamp, Index)]
		, TrainingUserNRelatedItemsA[, .(UserId, RelatedItemId = ItemId, RelatedTimestamp = Timestamp, RelatedIndex = Index)]
	, by = c("UserId"), allow.cartesian = T)
	TrainingUserNRelatedItemsA = TrainingUserNRelatedItemsA[ItemId != RelatedItemId]
	TrainingUserNRelatedItemsA[, UiriNoIndexDifference := abs(Index - RelatedIndex)]
	TrainingUserNRelatedItemsA = TrainingUserNRelatedItemsA[, .(UiriNoIndexDifference = sum(1 / UiriNoIndexDifference ** 4) ** 0.25), .(UserId, UserScore, ItemId, RelatedItemId)]

	TrainingUserNRelatedItemsA[, RelatingScore := (1 + UiriNoIndexDifference ** 4) / UserScore ** 0.2]
	TrainingUserNRelatedItemsA = TrainingUserNRelatedItemsA[, .(RelatingScore = sum(RelatingScore)), .(ItemId, RelatedItemId)]
	TrainingUserNRelatedItemsA = TrainingUserNRelatedItemsA[order(-RelatingScore)][1:(0.03 * nrow(TrainingUserNRelatedItemsA))]

	TrainingNRelatedItems = rbind(TrainingNRelatedItems, TrainingUserNRelatedItemsA)
}
write.table(TrainingNRelatedItems, "te4", sep = ",", row.names = F, col.names = F, quote = F)
