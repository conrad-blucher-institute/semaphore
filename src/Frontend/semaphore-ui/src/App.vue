<template>
  <div>
    <h1>Semaphore</h1>
    <DropdownMenu :options="menuOptions" @optionSelected="handleOptionSelected" />
  </div>
</template>

<script>
import DropdownMenu from './components/DropdownMenu.vue';

export default {
  name: "App",
  components: {
    DropdownMenu,
  },
  data() {
    return {
      menuOptions: ['Home'],
    };
  },
  methods: {
    handleOptionSelected(option) {
      console.log('Selected option:', option);
    },
  },
};
</script>

<style scoped>
/* Add styles here */
</style>
