package cn.mwee;

import java.util.*;

/**
 * @author tangyu
 * @since 2018-06-15 12:42
 */
public class BookDeskTimeExceptionMain extends Db {

    public static void main(String[] args) {
        new BookDeskTimeExceptionMain().execute();
    }

    void execute() {
        String timedeskShopIdSql = "SELECT distinct shopId  from orderbookstimedesk t";

        List<Map<String, Object>> reseult = this.getReseult(timedeskShopIdSql);

        List<Integer> shopIds = new ArrayList<>();
        reseult.forEach(x -> {
            Object shopId = x.get("shopId");
            shopIds.add((Integer) (shopId));
        });


        String timedeskSql = "SELECT id,t.deskIds,apm,wenkday from orderbookstimedesk t where shopId = ? ";
        String updateTimedeskSql = "update orderbookstimedesk set deskIds = ? where id = ?";
        String deleteTimedeskSql = "delete from orderbookstimedesk where id = ?";


        String orderbooksdeskSql = "SELECT deskId,t.shopId from orderbooksdesk t where t.shopId = ? ";
        for (Integer shopId : shopIds) {
            System.out.println("正在处理" + shopId);
            Integer[] shopIdParam = {shopId};
            List<Map<String, Object>> deskMapList = this.getReseult(orderbooksdeskSql, shopIdParam);

//            deskMapList.stream().flatMap(x -> {
//                Object deskId = x.get("deskId");
//                return deskId;
//            }).collect(String.);
            ArrayList<String> originDeskIds = new ArrayList<>();
            for (Map<String, Object> stringObjectMap : deskMapList) {
                String deskId = Objects.toString(stringObjectMap.get("deskId"), "0");
                originDeskIds.add(deskId);

            }

            List<Map<String, Object>> timedeskSqlResult = this.getReseult(timedeskSql, shopIdParam);
            timedeskSqlResult.forEach(x -> {
                String deskIds = (String) x.get("deskIds");
                Integer apm = (Integer) x.get("apm");
                Integer id = (Integer) x.get("id");
                Integer wenkday = (Integer) x.get("wenkday");
                String[] split = deskIds.split(",");
                List<String> checkableDeskIds = new ArrayList<>(Arrays.asList(split));
                List<String> invalidShopIds = new ArrayList<>();
                Iterator<String> iterator = checkableDeskIds.iterator();
                while (iterator.hasNext()) {
                    String s = iterator.next();
                    if (!originDeskIds.contains(s)) {
                        //TODO 记录需要删除Key
                        invalidShopIds.add(s);
                        iterator.remove();
                    }
                }

//                shopId:92961中桌位量17,星期2,市别1,自定义桌位量19其中异常桌位有2不包含桌位ID:[683617, 683622]
                String join = String.join(",", checkableDeskIds);
//                if (shopId == 8181) {
//                if (!invalidShopIds.isEmpty()) {
//                    if (join.isEmpty()) {
//                        delete(deleteTimedeskSql, new Object[]{id});
//                        System.out.println("删除------>>shopId:" + shopId + "中桌位量" + originDeskIds.size() + ",星期" + wenkday + ",市别" + apm + ",自定义桌位量" + split.length + "其中异常桌位有" + invalidShopIds.size() + "不包含桌位ID:" + invalidShopIds);
//                    } else {
//                        update(updateTimedeskSql, new Object[]{join, id});
//                        System.out.println("更新了------>>shopId:" + shopId + "中桌位量" + originDeskIds.size() + ",星期" + wenkday + ",市别" + apm + ",自定义桌位量" + split.length + "其中异常桌位有" + invalidShopIds.size() + "不包含桌位ID:" + invalidShopIds);
//                    }
//                }
//                } else {
                System.out.println("shopId:" + shopId + "中桌位量" + originDeskIds.size() + ",星期" + wenkday + ",市别" + apm + ",自定义桌位量" + split.length + "其中异常桌位有" + invalidShopIds.size() + "不包含桌位ID:" + invalidShopIds);
//                }
            });

        }
//        shopId:29636中桌位量50,星期1,市别2,自定义桌位量28其中异常桌位有14不包含桌位ID:[785991, 785992, 785994, 785995, 785993, 786011, 786027, 786028, 786029, 786030, 786032, 786031, 786033, 786034]
//        System.out.println(deskMapList.size());
//        deskMapList.forEach(x -> {
//            Object shopId = x.get("shopId");
//            System.out.println(shopId);
//        });

    }

}
