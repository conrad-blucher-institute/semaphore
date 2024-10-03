/* 
App.vue 
----------------------------------
 Created By: Anointiyae Beasley
 Created Date: 09/23/2024
 version 1.0
----------------------------------
App.Vue is the root component of the vue.js application. It serves as the main layout for the entire app.
App.vue also provides a place for all global elements ,like navigation bars, that will appear on every page.
----------------------------------
*/
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
