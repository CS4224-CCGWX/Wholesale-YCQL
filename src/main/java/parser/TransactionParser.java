package parser;


import com.datastax.driver.core.Session;
import transaction.*;

import java.util.ArrayList;
import java.util.Scanner;

public class TransactionParser {
    Scanner scanner = new Scanner(System.in);
    Session session;
    final String SEPARATOR = ",";

    public TransactionParser(Session session) {
        this.session = session;
    }

    public boolean hasNext() {
        return this.scanner.hasNext();
    }

    public void close() {
        this.scanner.close();
    }

    public AbstractTransaction parseNextTransaction() {
        if (!scanner.hasNext()) return null;

        String line = scanner.nextLine();
        String[] inputs = line.split(SEPARATOR);
        String txType = inputs[0];

        switch (txType) {
            case "N":
                return parseNewOrderTransaction(inputs);
            case "P":
                return parsePaymentTransaction(inputs);
            case "D":
                return parseDeliveryTransaction(inputs);
            case "O":
                return parseOrderStatusTransaction(inputs);
            case "S":
                return parseStockLevelTransaction(inputs);
            case "I":
                return parsePopularItemTransaction(inputs);
            case "T":
                return parseTopBalanceTransaction();
            case "R":
                return parseRelatedCustomerTransaction(inputs);
            default:
                throw new RuntimeException("Invalid type of transaction");
        }
    }

    private NewOrderTransaction parseNewOrderTransaction(String[] inputs) {
        int index = 1;
        int c_id = Integer.parseInt(inputs[index++]);
        int w_id = Integer.parseInt(inputs[index++]);
        int d_id = Integer.parseInt(inputs[index++]);

        int m = Integer.parseInt(inputs[index++]);

        ArrayList<Integer> i_ids = new ArrayList<>();
        ArrayList<Integer> w_ids = new ArrayList<>();
        ArrayList<Integer> quantities = new ArrayList<>();

        for (int i = 0; i < m; i++) {
            String line = scanner.nextLine();
            inputs = line.split(SEPARATOR);
            i_ids.add(Integer.parseInt(inputs[0]));
            w_ids.add(Integer.parseInt(inputs[1]));
            quantities.add(Integer.parseInt(inputs[2]));
        }
        return new NewOrderTransaction(session, c_id, w_id, d_id, m, i_ids, w_ids, quantities);
    }

    private PaymentTransaction parsePaymentTransaction(String[] inputs) {
        int index = 1;
        int w_id = Integer.parseInt(inputs[index++]);
        int d_id = Integer.parseInt(inputs[index++]);
        int c_id = Integer.parseInt(inputs[index++]);
        double payment = Double.parseDouble(inputs[index++]);

        return new PaymentTransaction(session, w_id, d_id, c_id, payment);
    }

    private DeliveryTransaction parseDeliveryTransaction(String[] inputs) {
        int index = 1;
        int w_id = Integer.parseInt(inputs[index++]);
        int carrier_id = Integer.parseInt(inputs[index++]);
        return new DeliveryTransaction(session, w_id, carrier_id);
    }

    private OrderStatusTransaction parseOrderStatusTransaction(String[] inputs) {
        int index = 1;
        int w_id = Integer.parseInt(inputs[index++]);
        int d_id = Integer.parseInt(inputs[index++]);
        int c_id = Integer.parseInt(inputs[index++]);
        return new OrderStatusTransaction(session, w_id, d_id, c_id);
    }

    private StockLevelTransaction parseStockLevelTransaction(String[] inputs) {
        int index = 1;
        int w_id = Integer.parseInt(inputs[index++]);
        int d_id = Integer.parseInt(inputs[index++]);
        double t = Double.parseDouble(inputs[index++]);
        int l = Integer.parseInt(inputs[index++]);

        return new StockLevelTransaction(session, w_id, d_id, t, l);
    }

    private PopularItemTransaction parsePopularItemTransaction(String[] inputs) {
        int index = 1;
        int w_id = Integer.parseInt(inputs[index++]);
        int d_id = Integer.parseInt(inputs[index++]);
        int l = Integer.parseInt(inputs[index++]);
        return new PopularItemTransaction(session, w_id, d_id, l);
    }

    private TopBalanceTransaction parseTopBalanceTransaction() {
        return new TopBalanceTransaction(session);
    }

    private RelatedCustomerTransaction parseRelatedCustomerTransaction(String[] inputs) {
        int index = 1;
        int w_id = Integer.parseInt(inputs[index++]);
        int d_id = Integer.parseInt(inputs[index++]);
        int c_id = Integer.parseInt(inputs[index++]);
        return new RelatedCustomerTransaction(session, w_id, d_id, c_id);
    }
}
