import type { GameState, PlayingState } from "./gamestate.js";

export function handlePause(gs: GameState, ps: PlayingState): void {
  console.log("Pause message received, ps.isPaused =", ps.isPaused);

  if (ps.isPaused) {
    console.log("==== Pause Detected ====");
    gs.pauseGame();
  } else {
    console.log("==== Resume Detected ====");
    gs.resumeGame();
  }
  console.log("------------------------");
}
