package com.example.wallet.domain;

import java.util.LinkedHashSet;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record Wallet(String id, int balance, LinkedHashSet<String> commandIds) { // <1>

  private static final Logger logger = LoggerFactory.getLogger(Wallet.class);
  public static final int COMMAND_IDS_MAX_SIZE = 1000;

  public static Wallet EMPTY = new Wallet("", 0, new LinkedHashSet<>());

  public boolean isEmpty() {
    return id.equals("");
  }

  public List<WalletEvent> handle(WalletCommand command) {
    if (commandIds.contains(command.commandId())) { // <2>
      logger.info("Command already processed: [{}]", command.commandId());
      return List.of();
    }
    return switch (command) {
      case WalletCommand.Deposit deposit -> List.of(
        new WalletEvent.Deposited(command.commandId(), deposit.amount())
      ); // <3>
      case WalletCommand.Withdraw withdraw -> List.of(
        new WalletEvent.Withdrawn(command.commandId(), withdraw.amount())
      ); // <3>
    };
  }

  public Wallet applyEvent(WalletEvent event) {
    return switch (event) {
      case WalletEvent.Created created -> new Wallet(
        created.walletId(),
        created.initialBalance(),
        new LinkedHashSet<>()
      );
      case WalletEvent.Withdrawn withdrawn -> new Wallet(
        id,
        balance - withdrawn.amount(),
        addCommandId(withdrawn.commandId())
      );
      case WalletEvent.Deposited deposited -> new Wallet(
        id,
        balance + deposited.amount(),
        addCommandId(deposited.commandId())
      );
    };
  }

  private LinkedHashSet<String> addCommandId(String commandId) {
    // To avoid infinite growth of the list with limit the size to 1000.
    // This implementation is not very efficient, so you might want to use a more dedicated data
    // structure for it.
    // When using other collections, make sure that the state is serializable and deserializable.
    // Another way to put some constraints on the list size is to remove commandIds based on time
    // e.g. remove commandIds that are older than 1 hour.
    if (commandIds.size() >= COMMAND_IDS_MAX_SIZE) { // <4>
      commandIds.removeFirst();
    }
    commandIds.add(commandId);
    return commandIds;
  }
}
