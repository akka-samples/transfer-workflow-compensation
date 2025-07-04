package com.example.transfer.application;

import akka.javasdk.annotations.ComponentId;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.workflow.Workflow;
import akka.javasdk.workflow.WorkflowContext;
import com.example.transfer.application.FraudDetectionService.FraudDetectionResult;
import com.example.transfer.domain.TransferState;
import com.example.transfer.domain.TransferState.Transfer;
import com.example.wallet.application.WalletEntity;
import com.example.wallet.application.WalletEntity.WalletResult;
import com.example.wallet.application.WalletEntity.WalletResult.Failure;
import com.example.wallet.application.WalletEntity.WalletResult.Success;
import com.example.wallet.domain.WalletCommand.Deposit;
import com.example.wallet.domain.WalletCommand.Withdraw;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.transfer.domain.TransferState.TransferStatus.COMPENSATION_COMPLETED;
import static com.example.transfer.domain.TransferState.TransferStatus.COMPLETED;
import static com.example.transfer.domain.TransferState.TransferStatus.DEPOSIT_FAILED;
import static com.example.transfer.domain.TransferState.TransferStatus.REQUIRES_MANUAL_INTERVENTION;
import static com.example.transfer.domain.TransferState.TransferStatus.TRANSFER_ACCEPTANCE_TIMED_OUT;
import static com.example.transfer.domain.TransferState.TransferStatus.WAITING_FOR_ACCEPTANCE;
import static com.example.transfer.domain.TransferState.TransferStatus.WITHDRAW_FAILED;
import static com.example.transfer.domain.TransferState.TransferStatus.WITHDRAW_SUCCEEDED;
import static java.time.Duration.ofHours;
import static java.time.Duration.ofSeconds;

@ComponentId("transfer") // <1>
public class TransferWorkflow extends Workflow<TransferState> {

  private static final Logger logger = LoggerFactory.getLogger(TransferWorkflow.class);

  private final ComponentClient componentClient;
  private final FraudDetectionService fraudDetectionService;
  private final String workflowId;

  public TransferWorkflow(
      ComponentClient componentClient,
      FraudDetectionService fraudDetectionService,
      WorkflowContext workflowContext) {
    this.componentClient = componentClient;
    this.fraudDetectionService = fraudDetectionService;
    this.workflowId = workflowContext.workflowId();
  }

  @Override
  public WorkflowDef<TransferState> definition() {
    return workflow()
      .timeout(ofSeconds(5)) // <1>
      .defaultStepTimeout(ofSeconds(2)) // <2>
      .failoverTo("failover-handler", maxRetries(0)) // <1>
      .defaultStepRecoverStrategy(maxRetries(1).failoverTo("failover-handler")) // <2>
      .addStep(detectFraudsStep())
      .addStep(withdrawStep())
      .addStep(depositStep(), maxRetries(2).failoverTo("compensate-withdraw")) // <3>
      .addStep(compensateWithdrawStep())
      .addStep(waitForAcceptanceStep())
      .addStep(failoverHandlerStep());
  }

  private Step detectFraudsStep() {
    return step("detect-frauds")
      .call(() -> { // <1>
        logger.info("Transfer {} - detecting frauds", currentState().transferId());
        return fraudDetectionService.detectFrauds(currentState().transfer());
      })
      .andThen(FraudDetectionResult.class, result -> {
        var transfer = currentState().transfer();
        return switch (result) {
          case ACCEPTED -> { // <2>
            logger.info("Running: " + transfer);
            TransferState initialState = TransferState.create(workflowId, transfer);
            Withdraw withdrawInput = new Withdraw(initialState.withdrawId(), transfer.amount());
            yield effects()
              .updateState(initialState)
              .transitionTo("withdraw", withdrawInput);
          }
          case MANUAL_ACCEPTANCE_REQUIRED -> { // <3>
            logger.info("Waiting for acceptance: " + transfer);
            TransferState waitingForAcceptanceState = TransferState.create(workflowId, transfer)
              .withStatus(WAITING_FOR_ACCEPTANCE);
            yield effects()
              .updateState(waitingForAcceptanceState)
              .transitionTo("wait-for-acceptance");
          }
        };
      });
  }

  private Step withdrawStep() {
    return
      step("withdraw")
        .call(Withdraw.class, cmd -> {
          logger.info("Running withdraw: {}", cmd);

          // cancelling the timer in case it was scheduled
          timers().delete("acceptanceTimout-" + currentState().transferId());

          return componentClient.forEventSourcedEntity(currentState().transfer().from())
            .method(WalletEntity::withdraw)
            .invoke(cmd);
        })
        .andThen(WalletResult.class, result -> {
          switch (result) {
            case Success __ -> {
              Deposit depositInput = new Deposit(
                  currentState().depositId(), currentState().transfer().amount());
              return effects()
                .updateState(currentState().withStatus(WITHDRAW_SUCCEEDED))
                .transitionTo("deposit", depositInput);
            }
            case Failure failure -> {
              logger.warn("Withdraw failed with msg: {}", failure.errorMsg());
              return effects()
                .updateState(currentState().withStatus(WITHDRAW_FAILED))
                .end();

            }
          }
        });
  }

  private Step depositStep() {
    return
      step("deposit")
        .call(Deposit.class, cmd -> {
          logger.info("Running deposit: {}", cmd);
          return componentClient.forEventSourcedEntity(currentState().transfer().to())
            .method(WalletEntity::deposit)
            .invoke(cmd);
        })
        .andThen(WalletResult.class, result -> { // <1>
          switch (result) {
            case Success __ -> {
              return effects()
                .updateState(currentState().withStatus(COMPLETED))
                .end(); // <2>
            }
            case Failure failure -> {
              logger.warn("Deposit failed with msg: {}", failure.errorMsg());
              return effects()
                .updateState(currentState().withStatus(DEPOSIT_FAILED))
                .transitionTo("compensate-withdraw"); // <3>
            }
          }
        });
  }

  private Step compensateWithdrawStep() {
    return
      step("compensate-withdraw") // <4>
        .call(() -> {
          logger.info("Running withdraw compensation");
          var transfer = currentState().transfer();
          // depositId is reused for the compensation, just to have a stable commandId and simplify the example
          String commandId = currentState().depositId();
          return componentClient.forEventSourcedEntity(transfer.from())
            .method(WalletEntity::deposit)
            .invoke(new Deposit(commandId, transfer.amount()));
        })
        .andThen(WalletResult.class, result -> {
          switch (result) {
            case Success __ -> {
              return effects()
                .updateState(currentState().withStatus(COMPENSATION_COMPLETED))
                .end(); // <5>
            }
            case Failure __ -> { // <6>
              throw new IllegalStateException("Expecting succeed operation but received: " + result);
            }
          }
        });
  }

  private Step waitForAcceptanceStep() {
    return step("wait-for-acceptance")
      .call(() -> {
        String transferId = currentState().transferId();
        timers().createSingleTimer(
          "acceptanceTimeout-" + transferId,
          ofHours(8),
          componentClient.forWorkflow(transferId)
            .method(TransferWorkflow::acceptanceTimeout)
            .deferred()); // <1>
      })
      .andThen(() ->
        effects().pause()); // <2>
  }

  private Step failoverHandlerStep() {
    Step failoverHandler =
      step("failover-handler")
        .call(() -> {
          logger.info("Running workflow failed step");
          return "handling failure";
        })
        .andThen(String.class, __ -> effects()
          .updateState(currentState().withStatus(REQUIRES_MANUAL_INTERVENTION))
          .end())
        .timeout(ofSeconds(1)); // <1>
    return failoverHandler;
  }

  public Effect<String> startTransfer(Transfer transfer) {
    if (currentState() != null) {
      return effects().error("transfer already started");
    } else if (transfer.amount() <= 0) {
      return effects().error("transfer amount should be greater than zero");
    } else {
      TransferState initialState = TransferState.create(workflowId, transfer);
      return effects()
        .updateState(initialState)
        .transitionTo("detect-frauds")
        .thenReply("transfer started");
    }
  }

  public Effect<String> acceptanceTimeout() {
    if (currentState() == null) {
      return effects().error("transfer not started");
    } else if (currentState().status() == WAITING_FOR_ACCEPTANCE) {
      return effects()
        .updateState(currentState().withStatus(TRANSFER_ACCEPTANCE_TIMED_OUT))
        .end()
        .thenReply("timed out");
    } else {
      logger.info("Ignoring acceptance timeout for status: " + currentState().status());
      return effects().reply("Ok");
    }
  }

  public Effect<String> accept() {
    if (currentState() == null) {
      return effects().error("transfer not started");
    } else if (currentState().status() == WAITING_FOR_ACCEPTANCE) { // <1>
      Transfer transfer = currentState().transfer();
      logger.info("Accepting transfer: " + transfer);
      Withdraw withdrawInput = new Withdraw(currentState().withdrawId(), transfer.amount());
      return effects()
        .transitionTo("withdraw", withdrawInput)
        .thenReply("transfer accepted");
    } else { // <2>
      return effects().error("Cannot accept transfer with status: " + currentState().status());
    }
  }

  public Effect<TransferState> getTransferState() {
    if (currentState() == null) {
      return effects().error("transfer not started");
    } else {
      return effects().reply(currentState());
    }
  }
}
