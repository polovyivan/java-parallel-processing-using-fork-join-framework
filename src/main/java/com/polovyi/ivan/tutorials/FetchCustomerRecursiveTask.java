package com.polovyi.ivan.tutorials;

import com.polovyi.ivan.tutorials.client.CustomerDataClient;
import com.polovyi.ivan.tutorials.client.CustomerPurchaseTransactionClient;
import com.polovyi.ivan.tutorials.dto.CustomerDataResponse;
import com.polovyi.ivan.tutorials.dto.CustomerWithTotalAmountSpend;
import com.polovyi.ivan.tutorials.dto.PurchaseTransactionResponse;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FetchCustomerRecursiveTask extends RecursiveTask<List<CustomerWithTotalAmountSpend>> {

    private static final int CHUNK_SIZE= 2;
    private List<Long> customerIds;
    private CustomerPurchaseTransactionClient customerPurchaseTransactionClient = new CustomerPurchaseTransactionClient();
    private CustomerDataClient customerDataClient = new CustomerDataClient();

    public FetchCustomerRecursiveTask(List<Long> customerIds) {
        this.customerIds = customerIds;
    }

    @Override
    protected List<CustomerWithTotalAmountSpend> compute() {
        int listSize = customerIds.size();

        if (listSize <= CHUNK_SIZE) {
            log.info("List has a size {} and is less than or equal to a chunk, PROCESSING a list.", listSize);
            return processIdList();
        }

        log.info("List has a size {} and is more than a chunk, SPLITTING a list.", listSize);
        int listMiddle = listSize / 2;
        log.info("Creating subtask...");
        FetchCustomerRecursiveTask leftSide = new FetchCustomerRecursiveTask(customerIds.subList(0, listMiddle));
        FetchCustomerRecursiveTask rightSide = new FetchCustomerRecursiveTask(
                customerIds.subList(listMiddle, listSize));
        log.info("Forking subtask...");
        leftSide.fork();
        rightSide.fork();

        log.info("Extracting the result from subtasks...");
        List<CustomerWithTotalAmountSpend> leftSideResult = leftSide.join();
        List<CustomerWithTotalAmountSpend> rightSideResult = rightSide.join();

        log.info("Combining results from subtasks and returning it...");
        return Stream.of(leftSideResult, rightSideResult)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

    }

    private List<CustomerWithTotalAmountSpend> processIdList() {
        return customerIds.stream()
                .map(this::fetchCustomerWithTotalAmountSpend)
                .collect(Collectors.toList());
    }

    private CustomerWithTotalAmountSpend fetchCustomerWithTotalAmountSpend(Long customerId) {

        List<PurchaseTransactionResponse> purchaseTransactionResponses = customerPurchaseTransactionClient.fetchByCustomerId(
                customerId);

        log.info("Summing up transactions... ");
        BigDecimal totalAmountSpend = purchaseTransactionResponses.stream()
                .map(PurchaseTransactionResponse::getAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        CustomerDataResponse customerDataResponse = customerDataClient.fetchCustomerById(customerId);

        return CustomerWithTotalAmountSpend.builder()
                .customerDataResponse(customerDataResponse)
                .totalAmountSpend(totalAmountSpend)
                .build();
    }
}
