package com.example.wallet.application;

import static akka.Done.done;

import akka.Done;
import akka.javasdk.annotations.ComponentId;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import com.example.wallet.application.WalletEntity.WalletResult.Failure;
import com.example.wallet.application.WalletEntity.WalletResult.Success;
import com.example.wallet.domain.Wallet;
import com.example.wallet.domain.WalletCommand.Deposit;
import com.example.wallet.domain.WalletCommand.Withdraw;
import com.example.wallet.domain.WalletEvent;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ComponentId("wallet")
public class WalletEntity extends EventSourcedEntity<Wallet, WalletEvent> {


  private static final Logger logger = LoggerFactory.getLogger(WalletEntity.class);

  @Override
  public Wallet emptyState() {
    return Wallet.EMPTY;
  }

  @Override
  public Wallet applyEvent(WalletEvent event) {
    return currentState().applyEvent(event);
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
  @JsonSubTypes(
    {
      @JsonSubTypes.Type(value = Success.class, name = "success"),
      @JsonSubTypes.Type(value = Failure.class, name = "failure"),
    }
  )
  public sealed interface WalletResult {
    record Failure(String errorMsg) implements WalletResult {}

    record Success() implements WalletResult {}
  }

  public Effect<Done> create(int initialBalance) {
    if (!currentState().isEmpty()) {
      return effects().error("Wallet already exists");
    } else {
      return effects()
        .persist(new WalletEvent.Created(commandContext().entityId(), initialBalance))
        .thenReply(__ -> done());
    }
  }

  public Effect<WalletResult> withdraw(Withdraw withdraw) {
    if (currentState().isEmpty()) {
      return effects().error("Wallet does not exist");
    } else if (currentState().balance() < withdraw.amount()) {
      return effects().reply(new Failure("Insufficient balance"));
    } else {
      logger.info(
        "Withdraw walletId: [{}] amount -{}",
        currentState().id(),
        withdraw.amount()
      );
      List<WalletEvent> events = currentState().handle(withdraw);
      return effects().persistAll(events).thenReply(__ -> new WalletResult.Success());
    }
  }

  public Effect<WalletResult> deposit(Deposit deposit) { // <1>
    if (currentState().isEmpty()) {
      return effects().error("Wallet does not exist");
    } else {
      logger.info("Deposit walletId: [{}] amount +{}", currentState().id(), deposit.amount());
      List<WalletEvent> events = currentState().handle(deposit);
      return effects().persistAll(events).thenReply(__ -> new WalletResult.Success());
    }
  }


  public Effect<Integer> get() {
    if (currentState().isEmpty()) {
      return effects().error("Wallet does not exist");
    } else {
      return effects().reply(currentState().balance());
    }
  }
}
