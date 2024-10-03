/* 
DropdownMenu.vue 
----------------------------------
 Created By: Anointiyae Beasley
 Created Date: 09/23/2024
 version 1.0
----------------------------------
This component constructs the drop down menu that will navigate to each page
----------------------------------
*/
<template>
  <!-- The button that displays 3 bars-->
    <div class="dropdown">
      <button class="dropbtn" @click="toggleDropdown">
        <div class="bar"></div>
        <div class="bar"></div>
        <div class="bar"></div>
      </button>
      <!-- If the dropdown menu is open, show the different options passed from App.vue
         and when one is clicked, the selectOption function is triggered -->
      <div class="dropdown-content" v-if="isOpen">
        <div v-for="option in options" :key="option" @click="selectOption(option)">
          {{ option }} <!-- Display each option -->
        </div>
      </div>
    </div>
  </template>
  
  <script>
  /* The default export for the dropdown menu component */
  export default {
    name: "DropdownMenu",
     /* options prop that defines the list of options for the dropdown menu */
    props: {
      options: {
        type: Array,
        required: true,
      },
    },
    data() {
      return {
        isOpen: false,  // Tracks if the dropdown is open or closed
        selectedOption: null, // Stores the currently selected option
      };
    },
    methods: {
      /* Toggles the visibility of the dropdown menu */
      toggleDropdown() {
        this.isOpen = !this.isOpen;
      },
      selectOption(option) {
        this.selectedOption = option;
        this.isOpen = false;
        this.$emit("optionSelected", option); 
      },
    },
  };
  </script>
  
  <style scoped>
  .dropdown {
    position: relative;
    display: inline-block;
    float: right; /* Align to the right */
    margin: 10px; /* Add some margin for spacing */
  }
  
  .dropbtn {
    background-color: transparent;
    border: none;
    cursor: pointer;
    padding: 10px; 
  }
  
  .bar {
    display: block;
    width: 25px; 
    height: 3px; /* Height of the hamburger bars */
    background-color: #333; /* Color of the bars */
    margin: 4px auto; /* Spacing between the bars */
  }
  
  .dropdown-content {
    display: block;
    position: absolute;
    right: 0; /* Align dropdown to the right */
    background-color: white;
    box-shadow: 0 8px 16px rgba(0, 0, 0, 0.2);
    z-index: 1;
    min-width: 160px;
  }
  
  .dropdown-content div {
    color: black;
    padding: 12px 16px;
    text-decoration: none;
    cursor: pointer;
  }
  
  .dropdown-content div:hover {
    background-color: #f1f1f1;
  }
  </style>
  