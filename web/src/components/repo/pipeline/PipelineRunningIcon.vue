<template>
  <StratimFlapIcon class="stratim-flap h-10 w-10 shrink-0 md:mx-1" />
</template>

<script lang="ts" setup>
import StratimFlapIcon from '~/assets/stratim-flap.svg?skipsvgo';
</script>

<style scoped>
/* Pivots are the wing shoulders in the SVG's own coordinates. */
.stratim-flap :deep(.wing-l),
.stratim-flap :deep(.wing-r),
.stratim-flap :deep(.bird) {
  transform-box: view-box;
  animation: 0.9s ease-in-out infinite;
}

.stratim-flap :deep(.wing-l) {
  transform-origin: 615px 609px;
  animation-name: flap-left;
}

.stratim-flap :deep(.wing-r) {
  transform-origin: 764px 672px;
  animation-name: flap-right;
}

.stratim-flap :deep(.bird) {
  animation-name: lift;
}

@keyframes flap-left {
  0%,
  100% {
    transform: rotate(6deg);
  }
  50% {
    transform: rotate(-22deg);
  }
}

@keyframes flap-right {
  0%,
  100% {
    transform: rotate(-6deg);
  }
  50% {
    transform: rotate(22deg);
  }
}

/* The downstroke lifts the bird a little. */
@keyframes lift {
  0%,
  100% {
    transform: translateY(0);
  }
  50% {
    transform: translateY(-25px);
  }
}

@media (prefers-reduced-motion: reduce) {
  .stratim-flap :deep(.wing-l),
  .stratim-flap :deep(.wing-r),
  .stratim-flap :deep(.bird) {
    animation: none;
  }
}
</style>
